package discovery

import (
	"context"

	"github.com/gotd/td/tg"
)

// CallLiveStatus is the result of verifying whether a group call is still active.
type CallLiveStatus int

const (
	CallLiveUnknown CallLiveStatus = iota
	CallLiveYes
	CallLiveNo
)

// VerifyCallStillLive checks PhoneGetGroupCall. Transient RPC errors return
// CallLiveUnknown so callers do not tear down ingest on network blips.
func VerifyCallStillLive(ctx context.Context, api *tg.Client, callID, accessHash int64) CallLiveStatus {
	if accessHash == 0 {
		return CallLiveUnknown
	}
	live, err := checkCallStillLive(ctx, api, callID, accessHash)
	if err != nil {
		if isGroupCallInvalid(err) {
			return CallLiveNo
		}
		return CallLiveUnknown
	}
	if live {
		return CallLiveYes
	}
	return CallLiveNo
}
