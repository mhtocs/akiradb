use lazy_static::lazy_static;
use regex::Regex;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct ApacheCombined {
    pub ipadress: Option<String>,
    pub username: Option<String>,
    pub date: Option<String>,
    // pub method: <Option<&'a str>>,
    pub request: Option<String>,
    pub status: Option<String>,
    pub size: Option<String>,
    pub referer: Option<String>,
    pub useragent: Option<String>,
}

pub fn parse(log: &str) -> Option<ApacheCombined> {
    lazy_static! {
        static ref R: Regex = Regex::new(
            r#"^(\d{1,3}+\.\d{1,3}+\.\d{1,3}+\.\d{1,3}+) - ([-\w]+) \[(.+?)\] "(([A-Z]+) (.+?))" (\d{3}) (\d+) "(.+?)" "(.+?)""#
        ).unwrap();
    }

    let s = R.captures(log);
    match s {
        Some(s) => Some(ApacheCombined {
            ipadress: Some(String::from(&s[1])),
            username: Some(String::from(&s[2])),
            date: Some(String::from(&s[3])),
            request: Some(String::from(&s[4])),
            status: Some(String::from(&s[7])),
            size: Some(String::from(&s[8])),
            referer: Some(String::from(&s[9])),
            useragent: Some(String::from(&s[10])),
        }),
        None => {
            println!("Failed to parse:: {}", log);
            None
        }
    }
}

#[cfg(test)]
mod tests {

    #[test]
    fn parse_test() {
        let log = r#"68.99.50.249 - - [12/Mar/2021:19:56:00 +0530] "HEAD /scale/sticky/interfaces HTTP/1.1" 100 1688 "https://www.dynamicone-to-one.com/seize/grow/wireless" "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_8_2) AppleWebKit/5311 (KHTML, like Gecko) Chrome/40.0.890.0 Mobile Safari/5311"
"#;

        let apache = super::ApacheCombined {
            ipadress: Some("68.99.50.249".to_owned()),
            username: Some("-".to_owned()),
            date: Some("12/Mar/2021:19:56:00 +0530".to_owned()),
            request: Some("HEAD /scale/sticky/interfaces HTTP/1.1".to_owned()),
            status: Some("100".to_owned()),
            size: Some("1688".to_owned()),
            referer: Some("https://www.dynamicone-to-one.com/seize/grow/wireless".to_owned()),
            useragent: Some("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_8_2) AppleWebKit/5311 (KHTML, like Gecko) Chrome/40.0.890.0 Mobile Safari/5311".to_owned()),
        };

        assert_eq!(super::parse(&log), Some(apache));
    }
}
