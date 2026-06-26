//! Pluggable access to large language model (LLM) providers.
//!
//! The [LlmProvider] trait abstracts over chat-completion style APIs so the rest of cimdea (for
//! example [crate::nl_tabulation]) can turn an English description into JSON without caring which
//! vendor answers. The first concrete implementation is [GeminiProvider] (Google Gemini); adding
//! Anthropic, OpenAI, or a local model is just another `impl LlmProvider`.
//!
//! [MockLlmProvider] returns a canned response and lets the orchestration logic be tested without
//! a network connection or an API key.

use crate::mderror::MdError;

/// The default Gemini model. Model ids change over time; override with [GeminiProvider::new] or the
/// `--model` CLI flag if this one is retired.
pub const DEFAULT_GEMINI_MODEL: &str = "gemini-2.5-flash";

const GEMINI_BASE_URL: &str = "https://generativelanguage.googleapis.com/v1beta";

/// POST a JSON body to `url` and return the response body as a string, mapping `ureq` transport and
/// HTTP-status errors to [MdError::LlmError]. Shared by the Gemini providers below.
fn post_json_for_text(url: &str, body: serde_json::Value) -> Result<String, MdError> {
    match ureq::post(url)
        .set("Content-Type", "application/json")
        .send_json(body)
    {
        Ok(resp) => resp.into_string().map_err(|err| {
            MdError::LlmError(format!("could not read the LLM response body: {err}"))
        }),
        Err(ureq::Error::Status(code, resp)) => {
            let detail = resp.into_string().unwrap_or_default();
            let hint = if code == 429 {
                " (rate limit / quota exceeded — free-tier limits are low and a filter/bin query \
                 uses an extra request; wait and retry, or enable billing on the project)"
            } else {
                ""
            };
            Err(MdError::LlmError(format!(
                "the LLM API returned HTTP {code}{hint}: {detail}"
            )))
        }
        Err(ureq::Error::Transport(transport)) => Err(MdError::LlmError(format!(
            "could not reach the LLM API: {transport}"
        ))),
    }
}

/// A minimal chat-completion interface over an LLM provider.
pub trait LlmProvider {
    /// Send a `system` instruction plus `user` content and return the model's reply as a string.
    ///
    /// Implementations should request JSON-only output where the underlying API supports it, but
    /// callers must still tolerate stray markdown fences (see
    /// [strip_json_fences](crate::nl_tabulation::strip_json_fences)).
    fn complete_json(&self, system: &str, user: &str) -> Result<String, MdError>;

    /// A human-readable identifier for the model, used in explanations and logs.
    fn model_name(&self) -> &str;
}

/// A [LlmProvider] backed by the Google Gemini `generateContent` REST endpoint.
pub struct GeminiProvider {
    api_key: String,
    model: String,
    base_url: String,
}

impl GeminiProvider {
    /// Create a provider with an explicit API key and model id.
    pub fn new(api_key: String, model: String) -> Self {
        Self {
            api_key,
            model,
            base_url: GEMINI_BASE_URL.to_string(),
        }
    }

    /// Create a provider reading the API key from the `GEMINI_API_KEY` environment variable.
    /// Pass `None` for `model` to use [DEFAULT_GEMINI_MODEL].
    pub fn from_env(model: Option<String>) -> Result<Self, MdError> {
        let api_key = std::env::var("GEMINI_API_KEY").map_err(|_| {
            MdError::LlmError(
                "GEMINI_API_KEY environment variable is not set (or pass --api-key)".to_string(),
            )
        })?;
        Ok(Self::new(
            api_key,
            model.unwrap_or_else(|| DEFAULT_GEMINI_MODEL.to_string()),
        ))
    }

    /// Override the base URL (used by tests pointing at a local mock server).
    pub fn with_base_url(mut self, base_url: String) -> Self {
        self.base_url = base_url;
        self
    }
}

impl LlmProvider for GeminiProvider {
    fn complete_json(&self, system: &str, user: &str) -> Result<String, MdError> {
        let url = format!(
            "{}/models/{}:generateContent?key={}",
            self.base_url, self.model, self.api_key
        );

        let request_body = serde_json::json!({
            "systemInstruction": { "parts": [ { "text": system } ] },
            "contents": [ { "role": "user", "parts": [ { "text": user } ] } ],
            "generationConfig": {
                "responseMimeType": "application/json",
                "temperature": 0.2
            }
        });

        let body = post_json_for_text(&url, request_body)?;

        let parsed: serde_json::Value = serde_json::from_str(&body).map_err(|err| {
            MdError::LlmError(format!(
                "Gemini response was not valid JSON ({err}); body was: {body}"
            ))
        })?;

        extract_gemini_text(&parsed)
    }

    fn model_name(&self) -> &str {
        &self.model
    }
}

/// Pull the generated text out of a Gemini `generateContent` response, concatenating all text
/// parts of the first candidate.
fn extract_gemini_text(response: &serde_json::Value) -> Result<String, MdError> {
    // If the prompt was blocked, Gemini returns promptFeedback with a blockReason and no candidates.
    if let Some(reason) = response
        .get("promptFeedback")
        .and_then(|f| f.get("blockReason"))
        .and_then(|r| r.as_str())
    {
        return Err(MdError::LlmError(format!(
            "Gemini blocked the prompt (reason: {reason})"
        )));
    }

    let parts = response
        .get("candidates")
        .and_then(|c| c.as_array())
        .and_then(|c| c.first())
        .and_then(|cand| cand.get("content"))
        .and_then(|content| content.get("parts"))
        .and_then(|parts| parts.as_array())
        .ok_or_else(|| {
            MdError::LlmError(format!(
                "Gemini response did not contain any candidates: {response}"
            ))
        })?;

    let text: String = parts
        .iter()
        .filter_map(|part| part.get("text").and_then(|t| t.as_str()))
        .collect();

    if text.is_empty() {
        return Err(MdError::LlmError(format!(
            "Gemini response candidate contained no text: {response}"
        )));
    }

    Ok(text)
}

/// A [LlmProvider] backed by the Gemini **Interactions API** (`POST /v1beta/interactions`), Google's
/// GA "recommended" interface. Functionally interchangeable with [GeminiProvider] for our one-shot
/// JSON use, but built for server-side conversation state (via `previous_interaction_id`), which we
/// will use for the Phase 2 interactive chat.
///
/// Note: the Interactions API's `response_format` enforces a JSON *schema*, and a bare
/// `{"type":"object"}` yields an empty object, so we don't use it here — our system prompt already
/// demands a JSON object and [crate::nl_tabulation::strip_json_fences] is the safety net.
pub struct InteractionsProvider {
    api_key: String,
    model: String,
    base_url: String,
}

impl InteractionsProvider {
    /// Create a provider with an explicit API key and model id.
    pub fn new(api_key: String, model: String) -> Self {
        Self {
            api_key,
            model,
            base_url: GEMINI_BASE_URL.to_string(),
        }
    }

    /// Create a provider reading the API key from the `GEMINI_API_KEY` environment variable.
    /// Pass `None` for `model` to use [DEFAULT_GEMINI_MODEL].
    pub fn from_env(model: Option<String>) -> Result<Self, MdError> {
        let api_key = std::env::var("GEMINI_API_KEY").map_err(|_| {
            MdError::LlmError(
                "GEMINI_API_KEY environment variable is not set (or pass --api-key)".to_string(),
            )
        })?;
        Ok(Self::new(
            api_key,
            model.unwrap_or_else(|| DEFAULT_GEMINI_MODEL.to_string()),
        ))
    }

    /// Override the base URL (used by tests pointing at a local mock server).
    pub fn with_base_url(mut self, base_url: String) -> Self {
        self.base_url = base_url;
        self
    }
}

impl LlmProvider for InteractionsProvider {
    fn complete_json(&self, system: &str, user: &str) -> Result<String, MdError> {
        let url = format!("{}/interactions?key={}", self.base_url, self.api_key);

        let request_body = serde_json::json!({
            "model": self.model,
            "system_instruction": system,
            "input": user,
            // One-shot translation: no need to persist conversation state on the server.
            "store": false,
        });

        let body = post_json_for_text(&url, request_body)?;

        let parsed: serde_json::Value = serde_json::from_str(&body).map_err(|err| {
            MdError::LlmError(format!(
                "Interactions response was not valid JSON ({err}); body was: {body}"
            ))
        })?;

        extract_interaction_text(&parsed)
    }

    fn model_name(&self) -> &str {
        &self.model
    }
}

/// Pull the generated text out of an Interactions `interactions.create` response. The reply is the
/// concatenated text of the `model_output` step(s); intermediate `thought` steps are ignored.
fn extract_interaction_text(response: &serde_json::Value) -> Result<String, MdError> {
    if let Some(err) = response.get("error") {
        return Err(MdError::LlmError(format!("Interactions API error: {err}")));
    }

    let steps = response
        .get("steps")
        .and_then(|s| s.as_array())
        .ok_or_else(|| {
            MdError::LlmError(format!("Interactions response had no steps: {response}"))
        })?;

    let mut text = String::new();
    for step in steps {
        if step.get("type").and_then(|t| t.as_str()) != Some("model_output") {
            continue;
        }
        if let Some(content) = step.get("content").and_then(|c| c.as_array()) {
            for part in content {
                if let Some(t) = part.get("text").and_then(|t| t.as_str()) {
                    text.push_str(t);
                }
            }
        }
    }

    if text.is_empty() {
        return Err(MdError::LlmError(format!(
            "Interactions response had no model_output text: {response}"
        )));
    }

    Ok(text)
}

/// A [LlmProvider] that always returns a fixed response. Useful for tests and offline development.
pub struct MockLlmProvider {
    response: String,
}

impl MockLlmProvider {
    pub fn new(response: impl Into<String>) -> Self {
        Self {
            response: response.into(),
        }
    }
}

impl LlmProvider for MockLlmProvider {
    fn complete_json(&self, _system: &str, _user: &str) -> Result<String, MdError> {
        Ok(self.response.clone())
    }

    fn model_name(&self) -> &str {
        "mock"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_gemini_text_concatenates_parts() {
        let response = serde_json::json!({
            "candidates": [
                { "content": { "parts": [ { "text": "{\"a\":" }, { "text": "1}" } ] } }
            ]
        });
        let text = extract_gemini_text(&response).expect("should extract text");
        assert_eq!(text, "{\"a\":1}");
    }

    #[test]
    fn test_extract_gemini_text_reports_block_reason() {
        let response = serde_json::json!({
            "promptFeedback": { "blockReason": "SAFETY" }
        });
        let err = extract_gemini_text(&response).expect_err("a blocked prompt should be an error");
        assert!(err.to_string().contains("SAFETY"));
    }

    #[test]
    fn test_extract_interaction_text_picks_model_output() {
        // A real response has a leading `thought` step followed by the `model_output` step.
        let response = serde_json::json!({
            "status": "completed",
            "steps": [
                { "type": "thought", "signature": "abc" },
                { "type": "model_output", "content": [ { "text": "{\"a\":", "type": "text" },
                                                        { "text": "1}", "type": "text" } ] }
            ]
        });
        let text = extract_interaction_text(&response).expect("should extract model_output text");
        assert_eq!(text, "{\"a\":1}");
    }

    #[test]
    fn test_extract_interaction_text_reports_error() {
        let response = serde_json::json!({
            "error": { "message": "bad request", "code": "invalid_request" }
        });
        let err = extract_interaction_text(&response).expect_err("an error body should be an error");
        assert!(err.to_string().contains("bad request"));
    }

    #[test]
    fn test_extract_interaction_text_no_output_is_error() {
        // Only a thought step, no model_output.
        let response = serde_json::json!({ "steps": [ { "type": "thought" } ] });
        assert!(extract_interaction_text(&response).is_err());
    }

    #[test]
    fn test_mock_provider_returns_canned_response() {
        let provider = MockLlmProvider::new("hello");
        assert_eq!(
            provider.complete_json("sys", "user").unwrap(),
            "hello".to_string()
        );
    }
}
