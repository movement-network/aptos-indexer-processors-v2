use bitflags::bitflags;
use std::collections::HashSet;
use tracing::warn;

bitflags! {
    #[derive(Debug, Clone, Copy, Eq, PartialEq)]
    pub struct TableFlags: u128 {
       // Default Processor: 1-10
        const TRANSACTIONS = 1 << 1;
        const WRITE_SET_CHANGES = 1 << 2;
        const MOVE_RESOURCES = 1 << 3;
        const TABLE_ITEMS = 1 << 4;
        const TABLE_METADATA = 1 << 5;
        const MOVE_MODULES = 1 << 6;
        const CURRENT_TABLE_ITEMS = 1 << 7;
        const BLOCK_METADATA_TRANSACTIONS = 1 << 8;

        // Fungible Asset Processor: 11-20
        const FUNGIBLE_ASSET_BALANCES = 1 << 11;
        const CURRENT_FUNGIBLE_ASSET_BALANCES = 1 << 12;
        const FUNGIBLE_ASSET_ACTIVITIES = 1 << 13;
        const FUNGIBLE_ASSET_METADATA = 1 << 14;
        const CURRENT_UNIFIED_FUNGIBLE_ASSET_BALANCES = 1 << 15;
        const CURRENT_FUNGIBLE_ASSET_BALANCES_LEGACY = 1 << 16;
        const FUNGIBLE_ASSET_TO_COIN_MAPPINGS = 1 << 17;
        // TODO:: Add new v1 to v2 fa mapping table when migrating fa processor

        // Objects Processor: 21-30
        const OBJECTS = 1 << 21;
        const CURRENT_OBJECTS = 1 << 22;

        // Ans Processor: 31-40
        const CURRENT_ANS_LOOKUP_V2 = 1 << 31;
        const CURRENT_ANS_PRIMARY_NAME_V2 = 1 << 32;
        const ANS_LOOKUP_V2 = 1 << 33;

        // Stake Processor: 41-50
        const DELEGATED_STAKING_ACTIVITIES = 1 << 41;
        const DELEGATED_STAKING_POOLS = 1 << 42;
        const DELEGATED_STAKING_POOL_BALANCES = 1 << 43;
        const CURRENT_DELEGATED_STAKING_POOL_BALANCES = 1 << 44;
        const DELEGATOR_BALANCES = 1 << 45;
        const CURRENT_DELEGATOR_BALANCES = 1 << 46;
        const CURRENT_DELEGATED_VOTER = 1 << 47;
        const CURRENT_STAKING_POOL_VOTER = 1 << 48;
        const PROPOSAL_VOTES = 1 << 49;

        // Token V2 Processor: 51-60
        const TOKEN_ACTIVITIES_V2 = 1 << 51;
        const CURRENT_TOKEN_OWNERSHIPS_V2 = 1 << 52;
        const CURRENT_TOKEN_DATAS_V2 = 1 << 53;
        const CURRENT_TOKEN_PENDING_CLAIMS = 1 << 54;
        const CURRENT_COLLECTIONS_V2 = 1 << 55;
        const CURRENT_TOKEN_V2_METADATA = 1 << 56;
        const COLLECTIONS_V2 = 1 << 57;
        const TOKEN_OWNERSHIPS_V2 = 1 << 58;
        const TOKEN_DATAS_V2 = 1 << 59;
        const CURRENT_TOKEN_ROYALTY_V1 = 1 << 60;

        // User Transactions and Signatures: 61-70
        const USER_TRANSACTIONS = 1 << 61;
        const SIGNATURES = 1 << 62;

        // Account Transaction Processor: 71-80
        const ACCOUNT_TRANSACTIONS = 1 << 71;

        // Events 81-90
        const EVENTS = 1 << 81;

        // transaction metadata 91-100
        const WRITE_SET_SIZE = 1 << 91;

        // Deprecated Tables 101-110
        const COIN_SUPPLY = 1 << 101;
        const CURRENT_ANS_LOOKUP = 1 << 102;
        const CURRENT_ANS_PRIMARY_NAME = 1 << 103;
        const ANS_PRIMARY_NAME_V2 = 1 << 104;
        const ANS_LOOKUP = 1 << 105;
        const ANS_PRIMARY_NAME = 1 << 106;

        // Account Restoration Processor: 111-120
        const AUTH_KEY_ACCOUNT_ADDRESSES = 1 << 111;
        const PUBLIC_KEY_AUTH_KEYS = 1 << 112;
        const GAS_FEES = 1 << 123;

        // Address Reputation Processor: 124-127
        const ADDRESS_TRANSFER_EDGES = 1 << 124;
        const BRIDGE_INFLOWS = 1 << 125;
        const ADDRESS_REPUTATION = 1 << 126;
    }
}

impl TableFlags {
    /// Tables that are NOT written unless they are named explicitly in `tables_to_write`.
    ///
    /// These are historical (non-current) tables holding one row per write set change, so
    /// they grow far faster than their `current_` counterparts. Leaving them off by default
    /// keeps existing deployments' storage profile unchanged; opt in per deployment.
    pub const OPT_IN_ONLY: Self = Self::TOKEN_DATAS_V2
        .union(Self::TOKEN_OWNERSHIPS_V2)
        .union(Self::FUNGIBLE_ASSET_BALANCES);

    /// Builds flags from the `tables_to_write` config entries. Names are matched
    /// case-insensitively so configs can use natural table names (`token_datas_v2`) rather
    /// than the uppercase flag names. Unrecognized names are skipped with a warning.
    pub fn from_set(set: &HashSet<String>) -> Self {
        let mut flags = TableFlags::empty();
        for table in set {
            match TableFlags::from_name(&table.to_uppercase()) {
                Some(flag) => flags |= flag,
                None => warn!(
                    table_name = table.as_str(),
                    "Unrecognized table name in tables_to_write, ignoring it"
                ),
            }
        }
        flags
    }
}

/**
 * This is a helper function to filter data based on the tables_to_write set.
 * If the tables_to_write set is empty or contains the flag, return the data so that they are written to the database.
 * Otherwise, return an empty vector so that they are not written to the database.
 *
 * Tables in `TableFlags::OPT_IN_ONLY` invert this: they are only written when named
 * explicitly, and naming one does not turn `tables_to_write` into an allowlist for
 * everything else. That way opting into a historical table doesn't silently switch off
 * every table that used to be written by default.
 */
pub fn filter_data<T>(tables_to_write: &TableFlags, flag: TableFlags, data: Vec<T>) -> Vec<T> {
    if TableFlags::OPT_IN_ONLY.contains(flag) {
        return if tables_to_write.contains(flag) {
            data
        } else {
            vec![]
        };
    }

    // Opt-in names don't count towards "did the operator specify an allowlist?".
    let allowlist = tables_to_write.difference(TableFlags::OPT_IN_ONLY);
    if allowlist.is_empty() || allowlist.contains(flag) {
        data
    } else {
        vec![]
    }
}

/// Macro to filter multiple data sets with their corresponding table flags in one go
#[macro_export]
macro_rules! filter_datasets {
    ($self:expr, { $($data:expr => $flag:expr),* $(,)? }) => {
        (
            $(
                filter_data(&$self.tables_to_write, $flag, $data),
            )*
        )
    };
}

#[cfg(test)]
mod tests {
    use super::*;

    fn set(names: &[&str]) -> HashSet<String> {
        names.iter().map(|s| (*s).to_string()).collect()
    }

    #[test]
    fn from_set_accepts_lowercase_table_names() {
        assert_eq!(
            TableFlags::from_set(&set(&["token_datas_v2", "CURRENT_OBJECTS"])),
            TableFlags::TOKEN_DATAS_V2 | TableFlags::CURRENT_OBJECTS
        );
    }

    #[test]
    fn from_set_ignores_unknown_names() {
        assert_eq!(
            TableFlags::from_set(&set(&["not_a_real_table"])),
            TableFlags::empty()
        );
    }

    #[test]
    fn empty_config_writes_default_tables_but_not_opt_in_ones() {
        let flags = TableFlags::empty();
        assert_eq!(
            filter_data(&flags, TableFlags::CURRENT_TOKEN_DATAS_V2, vec![1]),
            vec![1]
        );
        assert!(filter_data(&flags, TableFlags::TOKEN_DATAS_V2, vec![1]).is_empty());
        assert!(filter_data(&flags, TableFlags::FUNGIBLE_ASSET_BALANCES, vec![1]).is_empty());
    }

    #[test]
    fn opting_into_a_historical_table_keeps_other_defaults_on() {
        let flags = TableFlags::from_set(&set(&["token_datas_v2"]));
        assert_eq!(
            filter_data(&flags, TableFlags::TOKEN_DATAS_V2, vec![1]),
            vec![1]
        );
        // Not named, but still written because no non-opt-in allowlist was given.
        assert_eq!(
            filter_data(&flags, TableFlags::CURRENT_TOKEN_DATAS_V2, vec![1]),
            vec![1]
        );
        // A sibling opt-in table stays off.
        assert!(filter_data(&flags, TableFlags::TOKEN_OWNERSHIPS_V2, vec![1]).is_empty());
    }

    #[test]
    fn explicit_allowlist_still_excludes_unnamed_tables() {
        let flags = TableFlags::from_set(&set(&["current_token_datas_v2", "token_datas_v2"]));
        assert_eq!(
            filter_data(&flags, TableFlags::CURRENT_TOKEN_DATAS_V2, vec![1]),
            vec![1]
        );
        assert_eq!(
            filter_data(&flags, TableFlags::TOKEN_DATAS_V2, vec![1]),
            vec![1]
        );
        assert!(filter_data(&flags, TableFlags::CURRENT_TOKEN_OWNERSHIPS_V2, vec![1]).is_empty());
    }
}
