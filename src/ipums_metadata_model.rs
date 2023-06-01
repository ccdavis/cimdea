use std::collections::HashMap;

pub struct IpumsDataset {
    name: String,
    year: Option<usize>,
    month: Option<usize>,
    label: Option<String>,
    sample: Option<f64>,
    variables: Option<HashMap<String, IpumsDataset>>,
    id: usize, // auto-assigned in order loaded
}

pub enum IpumsDataType {
    Integer,
    Float,
    String,
    Fixed(usize)
}

pub struct IpumsVariable {
    name: String,
    data_type: IpumsDataType,
    label: Option<String>,
    record_type: String, // a value like 'H', 'P'
    datasets: Option<HashMap<String, IpumsDataset>>,
    id: usize, // auto-assigned in load order
}
