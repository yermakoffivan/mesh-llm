use serde_json::{Map, Value};

const MAX_PRESENTATION_CHARS: usize = 1024;

pub(super) fn safe_native_params(params: &[(String, Value)]) -> Map<String, Value> {
    params
        .iter()
        .filter(|(key, _)| safe_param_key(key))
        .filter_map(|(key, value)| match value {
            Value::Null | Value::Bool(_) | Value::Number(_) => Some((key.clone(), value.clone())),
            Value::String(value) => Some((key.clone(), Value::String(sanitize_text(value)))),
            Value::Array(_) | Value::Object(_) => None,
        })
        .collect()
}

pub(super) fn sanitize_map(mut fields: Map<String, Value>) -> Map<String, Value> {
    fields.retain(|key, value| {
        safe_param_key(key)
            && match value {
                Value::Null | Value::Bool(_) | Value::Number(_) => true,
                Value::String(text) => {
                    *text = sanitize_text(text);
                    true
                }
                Value::Array(_) | Value::Object(_) => false,
            }
    });
    fields
}

pub(super) fn sanitize_text(input: &str) -> String {
    let query_safe = redact_url_query_values(input);
    if contains_unredacted_credential(&query_safe) || contains_private_absolute_path(&query_safe) {
        return "[REDACTED]".to_string();
    }
    let normalized = query_safe.split_whitespace().collect::<Vec<_>>().join(" ");
    if normalized.chars().count() <= MAX_PRESENTATION_CHARS {
        normalized
    } else {
        format!(
            "{}... [TRUNCATED]",
            normalized
                .chars()
                .take(MAX_PRESENTATION_CHARS)
                .collect::<String>()
        )
    }
}

pub(super) fn native_category(category: &str) -> &'static str {
    match category {
        "backend" => "backend",
        "model" => "model",
        "memory" => "memory",
        "kv_cache" => "kv_cache",
        "tokenizer" => "tokenizer",
        _ => "runtime",
    }
}

pub(super) fn json_scalar(value: &Value) -> String {
    match value {
        Value::Null => "null".to_string(),
        Value::Bool(value) => value.to_string(),
        Value::Number(value) => value.to_string(),
        Value::String(value) => value.clone(),
        Value::Array(_) | Value::Object(_) => "[OMITTED]".to_string(),
    }
}

fn safe_param_key(key: &str) -> bool {
    key.len() <= 64
        && key
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || ch == '_')
        && !matches!(
            key,
            "timestamp"
                | "level"
                | "event"
                | "message"
                | "schema_version"
                | "event_id"
                | "request_id"
                | "attempt_id"
                | "channel"
                | "sequence"
                | "outcome"
        )
        && !is_sensitive_key(key)
        && !key.contains("path")
        && !key.contains("url")
        && !key.contains("id")
}

fn contains_private_absolute_path(input: &str) -> bool {
    input.split_whitespace().any(|word| {
        word.starts_with('/')
            || word.starts_with("file://")
            || std::env::var("HOME")
                .ok()
                .is_some_and(|home| !home.is_empty() && word.contains(&home))
    })
}

/// Apply the host logging policy's query-value rule before projecting text.
/// Endpoint and non-sensitive query metadata remain useful to an operator,
/// but credentials may not survive in any TUI surface.
fn redact_url_query_values(input: &str) -> String {
    input
        .split_whitespace()
        .map(redact_query_in_token)
        .collect::<Vec<_>>()
        .join(" ")
}

fn redact_query_in_token(token: &str) -> String {
    let Some((base, query_and_fragment)) = token.split_once('?') else {
        return token.to_string();
    };
    let (query, fragment) = query_and_fragment
        .split_once('#')
        .map_or((query_and_fragment, ""), |(query, fragment)| {
            (query, fragment)
        });
    let query = query
        .split('&')
        .filter(|parameter| !parameter.is_empty())
        .map(redact_query_parameter)
        .collect::<Vec<_>>()
        .join("&");
    let fragment_suffix = if fragment.is_empty() {
        String::new()
    } else {
        format!("#{fragment}")
    };

    if query.is_empty() {
        format!("{base}{fragment_suffix}")
    } else {
        format!("{base}?{query}{fragment_suffix}")
    }
}

fn redact_query_parameter(parameter: &str) -> String {
    let Some((key, value)) = parameter.split_once('=') else {
        return if is_sensitive_key(parameter) {
            format!("{parameter}=[REDACTED]")
        } else {
            parameter.to_string()
        };
    };

    if is_sensitive_key(key) || is_credential_value(value) {
        format!("{key}=[REDACTED]")
    } else {
        parameter.to_string()
    }
}

/// Match the host policy's credential categories by their semantic key shape.
/// This is deliberately broader than a presentation-only marker list, so new
/// token and credential parameter spellings fail closed in TUI projections.
fn is_sensitive_key(key: &str) -> bool {
    let key = key
        .trim_matches(|ch: char| matches!(ch, '"' | '\'' | ':' | ','))
        .to_ascii_lowercase();
    key.contains("token")
        || key.contains("password")
        || key.contains("secret")
        || key == "auth"
        || key.starts_with("auth_")
        || key == "bearer"
        || key == "key"
        || key.ends_with("key")
        || key == "session_id"
        || matches!(key.as_str(), "prompt" | "completion" | "response" | "query")
}

fn contains_unredacted_credential(input: &str) -> bool {
    let lower = input.to_ascii_lowercase();
    if lower.contains("bearer ")
        || lower.contains("basic ")
        || lower.contains("mesh-llm-")
        || lower.contains("sk_")
        || lower.contains("sk-")
        || lower.contains("ghp_")
    {
        return true;
    }

    input
        .split(|ch: char| ch.is_whitespace() || matches!(ch, '&' | ',' | '{' | '}'))
        .any(|segment| {
            let segment = segment.trim_matches(|ch: char| matches!(ch, '"' | '\''));
            is_credential_value(segment)
                || segment
                    .split_once(['=', ':'])
                    .is_some_and(|(key, value)| is_sensitive_key(key) && value != "[REDACTED]")
        })
}

fn is_credential_value(value: &str) -> bool {
    let value = value.trim().to_ascii_lowercase();
    value.starts_with("bearer ")
        || value.starts_with("basic ")
        || value.starts_with("mesh-llm-")
        || value.starts_with("sk_")
        || value.starts_with("sk-")
        || value.starts_with("ghp_")
}

#[cfg(test)]
mod tests {
    use super::sanitize_text;

    #[test]
    fn redacts_sensitive_url_query_values_without_hiding_safe_metadata() {
        let sanitized =
            sanitize_text("upstream provider.example/v1?token=supersecret&format=json&page=1");

        assert!(!sanitized.contains("supersecret"));
        assert!(sanitized.contains("token=[REDACTED]"));
        assert!(sanitized.contains("format=json"));
        assert!(sanitized.contains("page=1"));
    }

    #[test]
    fn redacts_the_operator_logging_privacy_corpus() {
        let home = std::env::var("HOME").expect("HOME should be set for privacy test");
        let cases = [
            (
                "provider endpoint https://provider.example/v1?access_token=secret-access&model=qwen"
                    .to_string(),
                "secret-access",
            ),
            (
                "callback provider.example/v1?api_key=secret-api-key&attempt=2".to_string(),
                "secret-api-key",
            ),
            (
                "authorization Bearer secret-bearer-token".to_string(),
                "secret-bearer-token",
            ),
            ("credential sk-supersecret-key".to_string(), "sk-supersecret-key"),
            (format!("native error at {home}/.mesh-llm/token"), home.as_str()),
        ];

        for (input, secret) in cases {
            let sanitized = sanitize_text(&input);
            assert!(
                !sanitized.contains(secret),
                "sanitized output leaked {secret:?}: {sanitized:?}"
            );
        }
    }
}
