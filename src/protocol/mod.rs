#[derive(Debug)]
pub enum Command {
    Ping,

    Set {
        key: String,
        value: String,
    },

    SetEx {
        key: String,
        value: String,
        ttl: u64,
    },

    Get {
        key: String,
    },

    Delete {
        key: String,
    },

    Metrics,

    Unknown,
}

pub fn parse(
    input: &str,
) -> Command {
    let parts: Vec<&str> =
        input
            .trim()
            .split_whitespace()
            .collect();

    if parts.is_empty() {
        return Command::Unknown;
    }

    match parts[0]
        .to_uppercase()
        .as_str()
    {
        "PING" => Command::Ping,

        "SET" => {
            if parts.len() != 3 {
                return Command::Unknown;
            }

            Command::Set {
                key: parts[1].to_string(),
                value: parts[2].to_string(),
            }
        }

        "SETEX" => {
            if parts.len() != 4 {
                return Command::Unknown;
            }

            let ttl =
                match parts[3].parse::<u64>()
                {
                    Ok(v) => v,

                    Err(_) => {
                        return Command::Unknown;
                    }
                };

            Command::SetEx {
                key: parts[1].to_string(),
                value: parts[2].to_string(),
                ttl,
            }
        }

        "GET" => {
            if parts.len() != 2 {
                return Command::Unknown;
            }

            Command::Get {
                key: parts[1].to_string(),
            }
        }

        "DEL" => {
            if parts.len() != 2 {
                return Command::Unknown;
            }

            Command::Delete {
                key: parts[1].to_string(),
            }
        }

        "METRICS" => {
            Command::Metrics
        }

        _ => Command::Unknown,
    }
}