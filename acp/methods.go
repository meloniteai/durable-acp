package acp

const (
	SchemaVersion       = "v1"
	SchemaRevision      = "4544546a94bc63a9719fa5ba84583e6726c7bd09"
	MethodCancelRequest = "$/cancel_request"

	MethodInitialize             = AgentMethodInitialize
	MethodAuthenticate           = AgentMethodAuthenticate
	MethodSessionNew             = AgentMethodSessionNew
	MethodSessionLoad            = AgentMethodSessionLoad
	MethodSessionSetMode         = AgentMethodSessionSetMode
	MethodSessionSetConfigOption = AgentMethodSessionSetConfigOption
	MethodSessionPrompt          = AgentMethodSessionPrompt
	MethodSessionCancel          = AgentMethodSessionCancel
	MethodSessionList            = AgentMethodSessionList
	MethodSessionDelete          = AgentMethodSessionDelete
	MethodSessionResume          = AgentMethodSessionResume
	MethodSessionClose           = AgentMethodSessionClose
	MethodLogout                 = AgentMethodLogout

	MethodSessionRequestPermission = ClientMethodSessionRequestPermission
	MethodSessionUpdate            = ClientMethodSessionUpdate
	MethodFsWriteTextFile          = ClientMethodFsWriteTextFile
	MethodFsReadTextFile           = ClientMethodFsReadTextFile
	MethodTerminalCreate           = ClientMethodTerminalCreate
	MethodTerminalOutput           = ClientMethodTerminalOutput
	MethodTerminalRelease          = ClientMethodTerminalRelease
	MethodTerminalWaitForExit      = ClientMethodTerminalWaitForExit
	MethodTerminalKill             = ClientMethodTerminalKill
	MethodElicitationCreate        = ClientMethodElicitationCreate
	MethodElicitationComplete      = ClientMethodElicitationComplete
)

type Peer string

const (
	PeerAgent  Peer = "agent"
	PeerClient Peer = "client"
	PeerEither Peer = "either"
)

type MessageKind string

const (
	MessageRequest      MessageKind = "request"
	MessageNotification MessageKind = "notification"
)

type Method struct {
	Name     string
	Receiver Peer
	Kind     MessageKind
}

var methods = [...]Method{
	{Name: MethodInitialize, Receiver: PeerAgent, Kind: MessageRequest},
	{Name: MethodAuthenticate, Receiver: PeerAgent, Kind: MessageRequest},
	{Name: MethodSessionNew, Receiver: PeerAgent, Kind: MessageRequest},
	{Name: MethodSessionLoad, Receiver: PeerAgent, Kind: MessageRequest},
	{Name: MethodSessionSetMode, Receiver: PeerAgent, Kind: MessageRequest},
	{Name: MethodSessionSetConfigOption, Receiver: PeerAgent, Kind: MessageRequest},
	{Name: MethodSessionPrompt, Receiver: PeerAgent, Kind: MessageRequest},
	{Name: MethodSessionCancel, Receiver: PeerAgent, Kind: MessageNotification},
	{Name: MethodSessionList, Receiver: PeerAgent, Kind: MessageRequest},
	{Name: MethodSessionDelete, Receiver: PeerAgent, Kind: MessageRequest},
	{Name: MethodSessionResume, Receiver: PeerAgent, Kind: MessageRequest},
	{Name: MethodSessionClose, Receiver: PeerAgent, Kind: MessageRequest},
	{Name: MethodLogout, Receiver: PeerAgent, Kind: MessageRequest},
	{Name: MethodSessionRequestPermission, Receiver: PeerClient, Kind: MessageRequest},
	{Name: MethodSessionUpdate, Receiver: PeerClient, Kind: MessageNotification},
	{Name: MethodFsWriteTextFile, Receiver: PeerClient, Kind: MessageRequest},
	{Name: MethodFsReadTextFile, Receiver: PeerClient, Kind: MessageRequest},
	{Name: MethodTerminalCreate, Receiver: PeerClient, Kind: MessageRequest},
	{Name: MethodTerminalOutput, Receiver: PeerClient, Kind: MessageRequest},
	{Name: MethodTerminalRelease, Receiver: PeerClient, Kind: MessageRequest},
	{Name: MethodTerminalWaitForExit, Receiver: PeerClient, Kind: MessageRequest},
	{Name: MethodTerminalKill, Receiver: PeerClient, Kind: MessageRequest},
	{Name: MethodElicitationCreate, Receiver: PeerClient, Kind: MessageRequest},
	{Name: MethodElicitationComplete, Receiver: PeerClient, Kind: MessageNotification},
	{Name: MethodCancelRequest, Receiver: PeerEither, Kind: MessageNotification},
}

func Methods() []Method {
	return append([]Method(nil), methods[:]...)
}
