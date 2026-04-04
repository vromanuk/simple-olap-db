use serde::Deserialize;

#[derive(Deserialize, Clone)]
pub struct Settings {
    pub application: ApplicationSettings,
    pub engine: EngineType,
    pub compaction: CompactionSettings,
}

#[derive(Deserialize, Clone)]
pub struct CompactionSettings {
    pub enabled: bool,
    pub interval_secs: u64,
    pub file_count_threshold: usize,
}

#[derive(Deserialize, Clone)]
pub struct ApplicationSettings {
    pub host: String,
    pub port: u16,
}

#[derive(Deserialize, Clone, Debug)]
#[serde(rename_all = "lowercase")]
pub enum EngineType {
    DataFusion,
    DuckDB,
}

pub enum Environment {
    Local,
    Production,
}

impl Environment {
    pub fn as_str(&self) -> &'static str {
        match self {
            Environment::Local => "local",
            Environment::Production => "production",
        }
    }
}

impl TryFrom<String> for Environment {
    type Error = String;

    fn try_from(s: String) -> Result<Self, Self::Error> {
        match s.to_lowercase().as_str() {
            "local" => Ok(Self::Local),
            "production" => Ok(Self::Production),
            other => Err(format!(
                "{other} is not a supported environment. Use `local` or `production`."
            )),
        }
    }
}

pub fn get_configuration() -> Result<Settings, config::ConfigError> {
    let base_path = std::env::current_dir().expect("failed to determine current directory");
    let configuration_directory = base_path.join("configuration");

    let environment: Environment = std::env::var("APP_ENVIRONMENT")
        .unwrap_or_else(|_| "local".into())
        .try_into()
        .expect("failed to parse APP_ENVIRONMENT");

    let settings = config::Config::builder()
        .add_source(config::File::from(configuration_directory.join("base")).required(true))
        .add_source(
            config::File::from(configuration_directory.join(environment.as_str())).required(true),
        )
        .add_source(
            config::Environment::with_prefix("APP")
                .prefix_separator("_")
                .separator("__"),
        )
        .build()?;

    settings.try_deserialize::<Settings>()
}
