use anyhow::Result;
use arrayvec::ArrayString;
use std::convert::TryFrom;

//based on https://nitschinger.at/Text-Analysis-in-Rust-Tokenization
//https://www.reddit.com/r/rust/comments/59wkgy/text_analysis_in_rust_tokenization/

const MAX_LEN: usize = 15;

#[derive(Debug, PartialEq, PartialOrd, Eq, Ord)]
pub enum Token {
    Stack(ArrayString<MAX_LEN>),
    Heap(String),
}

impl Token {
    #[inline]
    fn from_str(data: &str) -> Self {
        if data.len() <= MAX_LEN {
            Token::Stack(ArrayString::<MAX_LEN>::from(data).unwrap())
        } else {
            Token::Heap(data.into())
        }
    }

    #[inline]
    fn to_str(&self) -> &str {
        match self {
            Token::Stack(ref s) => s.as_ref(),
            Token::Heap(ref s) => s.as_ref(),
        }
    }
}

impl PartialEq<&str> for Token {
    fn eq(&self, other: &&str) -> bool {
        &self.to_str() == other
    }
}

impl AsRef<[u8]> for Token {
    fn as_ref(&self) -> &[u8] {
        self.to_str().as_bytes()
    }
}

pub trait Tokenizer<'a> {
    type TokenIterator: Iterator<Item = Token>;
    fn tokenize(&self, data: &'a str) -> Self::TokenIterator;
}

pub struct NGramTokenizer {
    ngram_size: usize,
}

impl NGramTokenizer {
    pub fn new(ngram_size: usize) -> Result<Self> {
        if ngram_size < 1 {
            return Err(anyhow::anyhow!("Can't be less than 1"));
        }

        Ok(NGramTokenizer { ngram_size })
    }
}

impl<'a> Tokenizer<'a> for NGramTokenizer {
    type TokenIterator = NGramIter<'a>;
    fn tokenize(&self, data: &'a str) -> Self::TokenIterator {
        NGramIter::new(self.ngram_size, data)
    }
}

#[derive(Debug, PartialEq)]
pub struct NGramIter<'a> {
    data: &'a str,
    ngram_size: usize,
    offset: usize,
    last_term_len: usize,
}

impl<'a> NGramIter<'a> {
    fn new(ngram_size: usize, data: &'a str) -> Self {
        NGramIter {
            data,
            ngram_size,
            offset: 0,
            last_term_len: 0,
        }
    }
}

impl<'a> Iterator for NGramIter<'a> {
    type Item = Token;

    //I dont know how this works now!
    fn next(&mut self) -> Option<Self::Item> {
        if self.offset + self.last_term_len > self.data.len() {
            return None;
        }

        let mut term_len = 0; //total count of all bytes for current term
        let offset = self.offset; //current starting position in data buffer

        for ch in self.data[self.offset..]
            .char_indices()
            // .by_ref()
            .take(self.ngram_size)
        {
            let char_len = ch.1.len_utf8();
            if ch.0 == 0 {
                self.offset += char_len;
            }
            term_len += char_len;
        }

        self.last_term_len = term_len;

        Some(Token::from_str(
            &self.data[offset..offset + term_len].to_lowercase(),
        ))
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    // use pretty_assertions::assert_eq;

    #[test]
    fn trigram_tokenizer_test() {
        let tokenizer = NGramTokenizer::new(3).unwrap();

        assert_eq!(
            tokenizer
                .tokenize("Abcdef ghij klmno")
                .collect::<Vec<Token>>(),
            vec![
                "abc", "bcd", "cde", "def", "ef ", "f g", " gh", "ghi", "hij", "ij ", "j k", " kl",
                "klm", "lmn", "mno"
            ]
        );

        assert_eq!(
            tokenizer.tokenize("123456789").collect::<Vec<Token>>(),
            vec!["123", "234", "345", "456", "567", "678", "789"]
        );

        //works on chinese characters too
        assert_eq!(
            tokenizer.tokenize("你好，世界！").collect::<Vec<Token>>(),
            ["你好，", "好，世", "，世界", "世界！"]
        );
    }
}
