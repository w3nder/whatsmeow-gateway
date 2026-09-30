package mapper

func (e InboundEvent) ChangesAnotherMessage() bool {
	switch e.Type {
	case "reaction", "edit", "revoke", "poll_vote":
		return true
	default:
		return false
	}
}
