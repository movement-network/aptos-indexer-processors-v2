ALTER TABLE proposal_votes DROP CONSTRAINT proposal_votes_pkey;
ALTER TABLE proposal_votes
ADD CONSTRAINT proposal_votes_pkey PRIMARY KEY (
    transaction_version,
    proposal_id,
    voter_address
  );
