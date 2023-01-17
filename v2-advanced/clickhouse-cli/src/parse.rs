use std::fmt;
use std::str::FromStr;

#[derive(Debug, Clone)]
pub struct SlotOrHash {
    pub slot: Option<u64>,
    pub hash: Option<String>,
}

impl fmt::Display for SlotOrHash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(slot) = self.slot {
            write!(f, "{}", slot)?;
        }

        if let Some(hash) = self.hash.as_ref() {
            write!(f, "{}", hash)?;
        }

        Ok(())
    }
}

impl SlotOrHash {
    pub fn from_slot_hash(slot: u64, hash: String) -> SlotOrHash {
        SlotOrHash {
            slot: Some(slot),
            hash: Some(hash),
        }
    }

    pub fn from_slot(slot: u64) -> SlotOrHash {
        SlotOrHash {
            slot: Some(slot),
            hash: None,
        }
    }

    pub fn from_hash(hash: String) -> SlotOrHash {
        SlotOrHash {
            slot: None,
            hash: Some(hash),
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct ParseError;

impl FromStr for SlotOrHash {
    type Err = ParseError;

    fn from_str(s: &str) -> Result<Self, ParseError> {
        let slot_number = s.parse::<u64>();
        let hash = bs58::decode(s).into_vec();

        match (slot_number, hash) {
            (Ok(slot), Ok(_hash)) => Ok(SlotOrHash::from_slot_hash(slot, s.to_string())),
            (Ok(slot), Err(_)) => Ok(SlotOrHash::from_slot(slot)),
            (Err(_), Ok(_hash)) => Ok(SlotOrHash::from_hash(s.to_string())),
            (Err(_), Err(_)) => Err(ParseError),
        }
    }
}

#[derive(Debug, Clone)]
pub struct SlotOrSignature {
    pub slot: Option<u64>,
    pub signature: Option<Vec<u8>>,
}

impl SlotOrSignature {
    pub fn from_slot_signature(slot: u64, pubkey: Vec<u8>) -> SlotOrSignature {
        SlotOrSignature {
            slot: Some(slot),
            signature: Some(pubkey),
        }
    }

    pub fn from_slot(slot: u64) -> SlotOrSignature {
        SlotOrSignature {
            slot: Some(slot),
            signature: None,
        }
    }

    pub fn from_signature(pubkey: Vec<u8>) -> SlotOrSignature {
        SlotOrSignature {
            slot: None,
            signature: Some(pubkey),
        }
    }
}

impl fmt::Display for SlotOrSignature {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(slot) = self.slot {
            write!(f, "{}", slot)?;
        }

        if let Some(signature) = self.signature.as_ref() {
            write!(f, "{:#?}", signature)?;
        }

        Ok(())
    }
}

impl FromStr for SlotOrSignature {
    type Err = ParseError;

    fn from_str(s: &str) -> Result<Self, ParseError> {
        let slot_number: Result<u64, ParseError> = s.parse::<u64>().map_err(|_| ParseError);
        let mut signature: Result<Vec<u8>, ParseError> = s[1..s.len() - 1]
            .split(',')
            .map(|s| s.parse::<u8>().map_err(|_| ParseError))
            .collect();

        if signature.is_err() {
            signature = bs58::decode(s).into_vec().map_err(|_| ParseError);
        }

        match (slot_number, signature) {
            (Ok(slot), Ok(signature)) => Ok(SlotOrSignature::from_slot_signature(slot, signature)),
            (Ok(slot), Err(_)) => Ok(SlotOrSignature::from_slot(slot)),
            (Err(_), Ok(signature)) => Ok(SlotOrSignature::from_signature(signature)),
            (Err(_), Err(_)) => Err(ParseError),
        }
    }
}

#[derive(Debug, Clone)]
pub struct VersionOrPubkey {
    pub write_version: Option<u64>,
    pub pubkey: Option<Vec<u8>>,
}

impl VersionOrPubkey {
    pub fn from_writev_signature(write_version: u64, pubkey: Vec<u8>) -> VersionOrPubkey {
        VersionOrPubkey {
            write_version: Some(write_version),
            pubkey: Some(pubkey),
        }
    }

    pub fn from_write_v(write_version: u64) -> VersionOrPubkey {
        VersionOrPubkey {
            write_version: Some(write_version),
            pubkey: None,
        }
    }

    pub fn from_signature(pubkey: Vec<u8>) -> VersionOrPubkey {
        VersionOrPubkey {
            write_version: None,
            pubkey: Some(pubkey),
        }
    }
}

impl fmt::Display for VersionOrPubkey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(write_version) = self.write_version {
            write!(f, "{}", write_version)?;
        }

        if let Some(pubkey) = self.pubkey.as_ref() {
            write!(f, "{:#?}", pubkey)?;
        }

        Ok(())
    }
}

impl FromStr for VersionOrPubkey {
    type Err = ParseError;

    fn from_str(s: &str) -> Result<Self, ParseError> {
        let write_version = s.parse::<u64>().map_err(|_| ParseError);
        let mut pubkey: Result<Vec<u8>, _> = s[1..s.len() - 1]
            .split(',')
            .map(|s| s.parse::<u8>().map_err(|_| ParseError))
            .collect();

        if pubkey.is_err() {
            pubkey = bs58::decode(s).into_vec().map_err(|_| ParseError);
        }

        match (write_version, pubkey) {
            (Ok(write_version), Ok(pubkey)) => Ok(VersionOrPubkey::from_writev_signature(
                write_version,
                pubkey,
            )),
            (Ok(write_version), Err(_)) => Ok(VersionOrPubkey::from_write_v(write_version)),
            (Err(_), Ok(pubkey)) => Ok(VersionOrPubkey::from_signature(pubkey)),
            (Err(_), Err(_)) => Err(ParseError),
        }
    }
}
