//! Common PostgreSQL binary scalar formats; unsupported OIDs return explicit errors.
use super::*;
type WireResult<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;
pub(super) fn input(oid: i32, bytes: &[u8]) -> WireResult<String> {
    Ok(match oid {
        PG_OID_BOOL if bytes.len() == 1 && bytes[0] <= 1 => if bytes[0] == 1 { "true" } else { "false" }.into(),
        PG_OID_INT2 if bytes.len() == 2 => i16::from_be_bytes(bytes.try_into()?).to_string(),
        PG_OID_INT4 if bytes.len() == 4 => i32::from_be_bytes(bytes.try_into()?).to_string(),
        PG_OID_INT8 if bytes.len() == 8 => i64::from_be_bytes(bytes.try_into()?).to_string(),
        PG_OID_FLOAT4 if bytes.len() == 4 => f32::from_be_bytes(bytes.try_into()?).to_string(),
        PG_OID_FLOAT8 if bytes.len() == 8 => f64::from_be_bytes(bytes.try_into()?).to_string(),
        PG_OID_TEXT | 1043 | 1042 => String::from_utf8(bytes.to_vec())?,
        _ => return Err(format!("binary parameter OID {oid} is not supported or has an invalid length; request text format").into()),
    })
}
fn output(oid: i32, bytes: &[u8]) -> WireResult<Vec<u8>> {
    let text = std::str::from_utf8(bytes)?;
    Ok(match oid {
        PG_OID_BOOL => vec![u8::from(matches!(text, "t" | "true"))],
        PG_OID_INT2 => text.parse::<i16>()?.to_be_bytes().to_vec(),
        PG_OID_INT4 => text.parse::<i32>()?.to_be_bytes().to_vec(),
        PG_OID_INT8 => text.parse::<i64>()?.to_be_bytes().to_vec(),
        PG_OID_FLOAT4 => text.parse::<f32>()?.to_be_bytes().to_vec(),
        PG_OID_FLOAT8 => text.parse::<f64>()?.to_be_bytes().to_vec(),
        PG_OID_TEXT => bytes.to_vec(),
        _ => {
            return Err(
                format!("binary result OID {oid} is not supported; request text format").into(),
            )
        }
    })
}
fn format(formats: &[usize], i: usize, n: usize) -> WireResult<usize> {
    if formats.len() > 1 && formats.len() != n {
        return Err("result format count does not match columns".into());
    }
    Ok(formats
        .get(if formats.len() == 1 { 0 } else { i })
        .copied()
        .unwrap_or(0))
}
pub(super) fn description(mut packet: Vec<u8>, formats: &[usize]) -> WireResult<Vec<u8>> {
    let n = u16::from_be_bytes(packet[5..7].try_into()?) as usize;
    let mut pos = 7;
    for i in 0..n {
        let name = packet[pos..]
            .iter()
            .position(|b| *b == 0)
            .ok_or("invalid column description")?;
        pos += name + 1;
        let value = format(formats, i, n)? as i16;
        packet[pos + 16..pos + 18].copy_from_slice(&value.to_be_bytes());
        pos += 18;
    }
    Ok(packet)
}
pub(super) fn row(packet: Vec<u8>, oids: &[i32], formats: &[usize]) -> WireResult<Vec<u8>> {
    let n = u16::from_be_bytes(packet[5..7].try_into()?) as usize;
    let mut pos = 7;
    if n != oids.len() {
        return Err("result column count changed".into());
    }
    let mut body = (n as i16).to_be_bytes().to_vec();
    for (i, oid) in oids.iter().enumerate() {
        let len = i32::from_be_bytes(packet[pos..pos + 4].try_into()?);
        pos += 4;
        if len == -1 {
            body.extend_from_slice(&(-1i32).to_be_bytes());
            continue;
        }
        let end = pos + len as usize;
        let bytes = &packet[pos..end];
        pos = end;
        let value = if format(formats, i, n)? == 1 {
            output(*oid, bytes)?
        } else {
            bytes.to_vec()
        };
        body.extend_from_slice(&(value.len() as i32).to_be_bytes());
        body.extend_from_slice(&value);
    }
    let mut output = vec![b'D'];
    output.extend_from_slice(&((body.len() + 4) as i32).to_be_bytes());
    output.extend(body);
    Ok(output)
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn binary_integer_and_boolean() {
        assert_eq!(input(PG_OID_INT4, &(-42i32).to_be_bytes()).unwrap(), "-42");
        assert_eq!(output(PG_OID_INT4, b"-42").unwrap(), (-42i32).to_be_bytes());
        assert_eq!(input(PG_OID_BOOL, &[1]).unwrap(), "true");
        assert!(input(PG_OID_BOOL, &[2]).is_err());
    }
}
