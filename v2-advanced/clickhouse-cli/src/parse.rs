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
pub struct ParseSlotOrHashError;

impl FromStr for SlotOrHash {
    type Err = ParseSlotOrHashError;

    fn from_str(s: &str) -> Result<Self, ParseSlotOrHashError> {
        let slot_number = s.parse::<u64>();
        let hash = bs58::decode(s).into_vec();

        match (slot_number, hash) {
            (Ok(slot), Ok(_hash)) => Ok(SlotOrHash::from_slot_hash(slot, s.to_string())),
            (Ok(slot), Err(_)) => Ok(SlotOrHash::from_slot(slot)),
            (Err(_), Ok(_hash)) => Ok(SlotOrHash::from_hash(s.to_string())),
            (Err(_), Err(_)) => Err(ParseSlotOrHashError),
        }
    }
}
