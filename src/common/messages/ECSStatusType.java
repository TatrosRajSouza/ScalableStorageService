package common.messages;

public enum ECSStatusType {
	INIT,
	START,
	STOP,
	SHUTDOWN,
	LOCK_WRITE,
	UNLOCK_WRITE,
	MOVE_DATA,
	UPDATE,	
	MOVE_COMPLETED,
	MOVE_DATA_INTERNAL,
	MOVE_DATA_INTERNAL_SUCCESS,
	MOVE_ERROR,
	FAILED
}