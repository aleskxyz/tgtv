package discovery

import (
	"context"
	"fmt"
	"time"

	"github.com/gotd/td/tg"
	"github.com/gotd/td/tgerr"
)

// RefreshLiveEntry re-validates a registry entry before ingest join.
func RefreshLiveEntry(ctx context.Context, api *tg.Client, registry *Registry, streamID string) (LiveEntry, error) {
	entry, ok := registry.Snapshot(streamID)
	if !ok {
		return LiveEntry{}, ErrUnknownStream
	}

	if entry.CallAccessHash != 0 {
		call := tg.InputGroupCall{ID: entry.CallID, AccessHash: entry.CallAccessHash}
		result, err := api.PhoneGetGroupCall(ctx, &tg.PhoneGetGroupCallRequest{Call: &call, Limit: 1})
		if err == nil {
			if gc, ok := result.Call.(*tg.GroupCall); ok && gc.ScheduleDate == 0 && gc.ID == entry.CallID {
				if snap, ok, verifyErr := verifyCachedCall(ctx, api, registry, streamID, entry); verifyErr == nil && ok {
					return snap, nil
				} else if verifyErr != nil {
					if wait, ok := tgerr.AsFloodWait(verifyErr); ok {
						return trustCachedLive(registry, streamID, entry, wait)
					}
					if isGroupCallInvalid(verifyErr) {
						return markEndedAndFail(registry, entry.ChatID, "call ended")
					}
					// fall through to full refresh on verify failure
				}
			}
		} else if wait, ok := tgerr.AsFloodWait(err); ok {
			return trustCachedLive(registry, streamID, entry, wait)
		} else if isGroupCallInvalid(err) {
			return markEndedAndFail(registry, entry.ChatID, "call ended")
		} else {
			return useCachedLive(registry, streamID, entry)
		}
	}

	chat, err := fetchChatEntity(ctx, api, registry, entry.ChatID)
	if err != nil {
		if wait, ok := tgerr.AsFloodWait(err); ok {
			return trustCachedLive(registry, streamID, entry, wait)
		}
		if isGroupCallInvalid(err) {
			return markEndedAndFail(registry, entry.ChatID, "call ended")
		}
		return useCachedLive(registry, streamID, entry)
	}
	if chat == nil {
		return markEndedAndFail(registry, entry.ChatID, "chat not found")
	}

	inputCall, live, err := fetchActiveCall(ctx, api, chat)
	if err != nil {
		if wait, ok := tgerr.AsFloodWait(err); ok {
			return trustCachedLive(registry, streamID, entry, wait)
		}
		if isGroupCallInvalid(err) {
			return markEndedAndFail(registry, entry.ChatID, "call ended")
		}
		return useCachedLive(registry, streamID, entry)
	}
	if !live {
		if entry.CallAccessHash != 0 {
			stillLive, err := checkCallStillLive(ctx, api, entry.CallID, entry.CallAccessHash)
			if err != nil {
				if isGroupCallInvalid(err) {
					return markEndedAndFail(registry, entry.ChatID, "call ended")
				}
				if wait, ok := tgerr.AsFloodWait(err); ok {
					return trustCachedLive(registry, streamID, entry, wait)
				}
				return useCachedLive(registry, streamID, entry)
			}
			if stillLive {
				return entry, nil
			}
		}
		return markEndedAndFail(registry, entry.ChatID, "no active call")
	}

	if !registry.UpdateCallInfo(streamID, inputCall.ID, inputCall.AccessHash) {
		return LiveEntry{}, fmt.Errorf("stream no longer active")
	}
	registry.Reactivate(streamID)
	return registry.snapshotLocked(streamID)
}

func isGroupCallInvalid(err error) bool {
	return err != nil && tgerr.Is(err, "GROUPCALL_INVALID")
}

// verifyCachedCall confirms the chat's active call still matches the registry entry.
func verifyCachedCall(ctx context.Context, api *tg.Client, registry *Registry, streamID string, entry LiveEntry) (LiveEntry, bool, error) {
	chat, err := fetchChatEntity(ctx, api, registry, entry.ChatID)
	if err != nil {
		return LiveEntry{}, false, err
	}
	if chat == nil {
		return LiveEntry{}, false, nil
	}
	inputCall, live, err := fetchActiveCall(ctx, api, chat)
	if err != nil || !live {
		return LiveEntry{}, false, err
	}
	if inputCall.ID != entry.CallID {
		if !registry.UpdateCallInfo(streamID, inputCall.ID, inputCall.AccessHash) {
			return LiveEntry{}, false, fmt.Errorf("stream no longer active")
		}
	}
	registry.Reactivate(streamID)
	snap, err := registry.snapshotLocked(streamID)
	return snap, err == nil, err
}

func trustCachedLive(registry *Registry, streamID string, entry LiveEntry, wait time.Duration) (LiveEntry, error) {
	entry = freshLiveEntry(registry, streamID, entry)
	if entry.CallAccessHash == 0 {
		return LiveEntry{}, fmt.Errorf("flood wait %s with no cached call", wait)
	}
	registry.Reactivate(streamID)
	return entry, nil
}

// useCachedLive returns the cached entry on transient API errors (LivePlayer poll2
// reschedules instead of declaring the call dead unless GROUPCALL_INVALID).
func useCachedLive(registry *Registry, streamID string, entry LiveEntry) (LiveEntry, error) {
	entry = freshLiveEntry(registry, streamID, entry)
	if entry.CallAccessHash == 0 {
		return LiveEntry{}, fmt.Errorf("no cached call")
	}
	return entry, nil
}

func freshLiveEntry(registry *Registry, streamID string, entry LiveEntry) LiveEntry {
	if registry == nil || streamID == "" {
		return entry
	}
	if fresh, ok := registry.Snapshot(streamID); ok {
		return fresh
	}
	return entry
}

func markEndedAndFail(registry *Registry, chatID int64, msg string) (LiveEntry, error) {
	registry.MarkEnded(chatID)
	return LiveEntry{}, &StreamEndedError{ChatID: chatID, Reason: msg}
}

func fetchChatEntity(ctx context.Context, api *tg.Client, registry *Registry, chatID int64) (tg.ChatClass, error) {
	hash, ok := registry.ChannelAccessHash(chatID)
	if !ok {
		return nil, nil
	}
	result, err := api.ChannelsGetChannels(ctx, []tg.InputChannelClass{
		&tg.InputChannel{ChannelID: chatID, AccessHash: hash},
	})
	if err != nil {
		return nil, err
	}
	chats := result.GetChats()
	if len(chats) == 0 {
		return nil, nil
	}
	ch, ok := chats[0].(*tg.Channel)
	if !ok {
		return nil, nil
	}
	registry.RememberChannelAccess(ch.ID, ch.AccessHash)
	return ch, nil
}

func fetchActiveCall(ctx context.Context, api *tg.Client, chat tg.ChatClass) (tg.InputGroupCall, bool, error) {
	c, ok := chat.(*tg.Channel)
	if !ok {
		return tg.InputGroupCall{}, false, nil
	}
	full, err := api.ChannelsGetFullChannel(ctx, c.AsInput())
	if err != nil {
		return tg.InputGroupCall{}, false, err
	}
	fc, ok := full.FullChat.(*tg.ChannelFull)
	if !ok {
		return tg.InputGroupCall{}, false, nil
	}
	input, ok := fc.GetCall()
	if !ok {
		return tg.InputGroupCall{}, false, nil
	}
	inputCall, ok := inputGroupCallClass(input)
	if !ok {
		return tg.InputGroupCall{}, false, nil
	}

	result, err := api.PhoneGetGroupCall(ctx, &tg.PhoneGetGroupCallRequest{
		Call:  &inputCall,
		Limit: 1,
	})
	if err != nil {
		return tg.InputGroupCall{}, false, err
	}
	if !isLiveGroupCall(result.Call) {
		return tg.InputGroupCall{}, false, nil
	}
	return inputCall, true, nil
}

func checkCallStillLive(ctx context.Context, api *tg.Client, callID, accessHash int64) (bool, error) {
	if accessHash == 0 {
		return false, nil
	}
	call := tg.InputGroupCall{ID: callID, AccessHash: accessHash}
	result, err := api.PhoneGetGroupCall(ctx, &tg.PhoneGetGroupCallRequest{Call: &call, Limit: 1})
	if err != nil {
		return false, err
	}
	return isLiveGroupCall(result.Call), nil
}
