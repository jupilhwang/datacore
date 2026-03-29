// Consumer Group Domain Tests
module domain

fn test_group_state_str_empty() {
	assert GroupState.empty.str() == 'Empty'
}

fn test_group_state_str_preparing_rebalance() {
	assert GroupState.preparing_rebalance.str() == 'PreparingRebalance'
}

fn test_group_state_str_completing_rebalance() {
	assert GroupState.completing_rebalance.str() == 'CompletingRebalance'
}

fn test_group_state_str_stable() {
	assert GroupState.stable.str() == 'Stable'
}

fn test_group_state_str_dead() {
	assert GroupState.dead.str() == 'Dead'
}
