-- batch_vote / batch_partial_vote emit one VoteEvent per stake pool in a single
-- transaction. The old PK (transaction_version, proposal_id, voter_address)
-- collapsed those rows and ON CONFLICT DO NOTHING dropped all but the first.
ALTER TABLE proposal_votes DROP CONSTRAINT proposal_votes_pkey;
ALTER TABLE proposal_votes
ADD CONSTRAINT proposal_votes_pkey PRIMARY KEY (
    transaction_version,
    proposal_id,
    voter_address,
    staking_pool_address
  );
