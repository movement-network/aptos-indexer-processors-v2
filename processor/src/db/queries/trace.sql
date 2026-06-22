-- Backward trace from an Aptos address through transfer edges.
-- Stops when an edge marked is_bridge_inflow is reached, or when max depth is exceeded.
-- Params: $1 = target address, $2 = max depth (use a large int for "full trace").

WITH RECURSIVE upstream AS (
    SELECT
        e.to_address                AS sink,
        e.from_address              AS source,
        e.transaction_version,
        e.event_index,
        e.amount,
        e.is_bridge_inflow,
        e.bridge_name,
        0                            AS depth
    FROM address_transfer_edges e
    WHERE e.to_address = $1
  UNION ALL
    SELECT
        e.to_address,
        e.from_address,
        e.transaction_version,
        e.event_index,
        e.amount,
        e.is_bridge_inflow,
        e.bridge_name,
        u.depth + 1
    FROM address_transfer_edges e
    JOIN upstream u ON e.to_address = u.source
    WHERE u.depth < $2 AND NOT u.is_bridge_inflow
)
SELECT
    sink,
    source,
    transaction_version,
    event_index,
    amount,
    is_bridge_inflow,
    bridge_name,
    depth
FROM upstream
ORDER BY depth, transaction_version, event_index;
