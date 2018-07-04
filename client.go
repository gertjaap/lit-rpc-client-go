package litrpcclient

import (
	"fmt"
	"net/rpc"
	"net/rpc/jsonrpc"
	"strconv"
	"strings"

	"github.com/mit-dci/lit/dlc"
	"github.com/mit-dci/lit/litrpc"
	"github.com/mit-dci/lit/lnutil"
	"github.com/mit-dci/lit/qln"
	"golang.org/x/net/websocket"
)

type LitRpcClient struct {
	wsConn          *websocket.Conn
	rpcConn         *rpc.Client
	listeningStatus uint8
}

// NewClient creates a new LitRpcClient and connects to the given
// hostname and port
func NewClient(host string, port int32) (*LitRpcClient, error) {
	client := new(LitRpcClient)
	var err error
	client.wsConn, err = websocket.Dial(fmt.Sprintf("ws://%s:%d/ws", host, port), "", "http://127.0.0.1/")
	if err != nil {
		return nil, err
	}
	client.rpcConn = jsonrpc.NewClient(client.wsConn)
	return client, nil
}

// Close Disconnects from the LIT node
func (c *LitRpcClient) Close() {
	c.wsConn.Close()
}

//Listen instructs LIT to listen for incoming connections. By default, LIT will not
//listen. If LIT was already listening for incoming connections, this method
//will just resolve.
func (c *LitRpcClient) Listen(port string) error {
	args := new(litrpc.ListenArgs)
	args.Port = port

	reply := new(litrpc.ListeningPortsReply)
	err := c.rpcConn.Call("LitRPC.Listen", args, reply)
	if err != nil {
		if strings.Index(err.Error(), "already in use") == -1 {
			return err
		}
	}
	c.listeningStatus = 1
	return nil
}

// IsListening checks if LIT is currently listening on any port.
func (c *LitRpcClient) IsListening() (bool, error) {
	if c.listeningStatus > 0 {
		return (c.listeningStatus == 1), nil
	}

	args := new(litrpc.NoArgs)
	reply := new(litrpc.ListeningPortsReply)
	err := c.rpcConn.Call("LitRPC.GetListeningPorts", args, reply)
	if err != nil {
		return false, err
	}
	c.listeningStatus = 1
	if reply.LisIpPorts == nil {
		c.listeningStatus = 2
	}
	return (c.listeningStatus == 1), nil
}

// GetLNAddress returns the LN address for this node
func (c *LitRpcClient) GetLNAddress() (string, error) {
	args := new(litrpc.NoArgs)

	reply := new(litrpc.ListeningPortsReply)
	err := c.rpcConn.Call("LitRPC.GetListeningPorts", args, reply)
	if err != nil {
		return "", err
	}
	return reply.Adr, nil
}

// Connect connects to another LIT node. address is mandatory, host and port can be left empty / 0.
func (c *LitRpcClient) Connect(address, host string, port uint32) error {
	args := new(litrpc.ConnectArgs)
	args.LNAddr = address
	reply := new(litrpc.StatusReply)
	if host != "" {
		args.LNAddr += "@" + host
		if port != 2448 && port != 0 {
			args.LNAddr += ":" + strconv.Itoa(int(port))
		}
	}
	err := c.rpcConn.Call("LitRPC.Connect", args, reply)
	if err != nil {
		return err
	}
	if strings.Index(reply.Status, "connected to peer") == -1 {
		return fmt.Errorf("Unexpected response from server: %s", reply.Status)
	}
	return nil
}

// ListConnections Returns a list of currently connected nodes
func (c *LitRpcClient) ListConnections() ([]qln.PeerInfo, error) {
	empty := make([]qln.PeerInfo, 0)
	args := new(litrpc.NoArgs)

	reply := new(litrpc.ListConnectionsReply)
	err := c.rpcConn.Call("LitRPC.ListConnections", args, reply)
	if err != nil {
		return empty, err
	}
	if reply.Connections == nil {
		return empty, nil
	}

	return reply.Connections, nil
}

// AssignNickname assigns the nickname [nickname] to the known peer with index [peerIndex]
func (c *LitRpcClient) AssignNickname(peerIndex uint32, nickname string) error {
	args := new(litrpc.AssignNicknameArgs)
	args.Peer = peerIndex
	args.Nickname = nickname
	reply := new(litrpc.StatusReply)
	err := c.rpcConn.Call("LitRPC.AssignNickname", args, reply)
	if err != nil {
		return err
	}
	if strings.Index(reply.Status, "changed nickname") == -1 {
		return fmt.Errorf("Unexpected response from server: %s", reply.Status)
	}
	return nil
}

// Stop stops the LIT node. This means you'll have to restart it manually.
// After stopping the node you can no longer connect to it via RPC.
func (c *LitRpcClient) Stop() error {
	args := new(litrpc.NoArgs)
	reply := new(litrpc.StatusReply)
	err := c.rpcConn.Call("LitRPC.Stop", args, reply)
	if err != nil {
		return err
	}
	if strings.Index(reply.Status, "Stopping lit node") == -1 {
		return fmt.Errorf("Unexpected response from server: %s", reply.Status)
	}
	return nil
}

// Returns a list of balances from the LIT node's wallet
func (c *LitRpcClient) ListBalances() ([]litrpc.CoinBalReply, error) {
	empty := make([]litrpc.CoinBalReply, 0)
	args := new(litrpc.NoArgs)

	reply := new(litrpc.BalanceReply)
	err := c.rpcConn.Call("LitRPC.Balance", args, reply)
	if err != nil {
		return empty, err
	}
	if reply.Balances == nil {
		return empty, nil
	}

	return reply.Balances, nil
}

// Returns a list of all unspent transaction outputs, that are not part of a channel
func (c *LitRpcClient) ListUtxos() ([]litrpc.TxoInfo, error) {
	empty := make([]litrpc.TxoInfo, 0)
	args := new(litrpc.NoArgs)

	reply := new(litrpc.TxoListReply)
	err := c.rpcConn.Call("LitRPC.TxoList", args, reply)
	if err != nil {
		return empty, err
	}
	if reply.Txos == nil {
		return empty, nil
	}

	return reply.Txos, nil
}

// Send sends coins from LIT's wallet using a normal on-chain transaction. Send to [address]
// [amount] coins. Will return the transaction ID of the on-chain transaction
func (c *LitRpcClient) Send(address string, amount int64) (string, error) {
	args := new(litrpc.SendArgs)
	args.Amts = []int64{amount}
	args.DestAddrs = []string{address}
	reply := new(litrpc.TxidsReply)
	err := c.rpcConn.Call("LitRPC.Send", args, reply)
	if err != nil {
		return "", err
	}
	if reply.Txids == nil {
		return "", fmt.Errorf("Unexpected response from server")
	}

	return reply.Txids[0], nil
}

// SetFee allows you to configure the fee rate for a particular coin type. It will set
// the fee for [coinType] to [feePerByte] satoshi/byte
func (c *LitRpcClient) SetFee(coinType uint32, feePerByte int64) error {
	args := new(litrpc.SetFeeArgs)
	args.CoinType = coinType
	args.Fee = feePerByte
	reply := new(litrpc.FeeReply)
	err := c.rpcConn.Call("LitRPC.SetFee", args, reply)
	if err != nil {
		return err
	}
	if reply.CurrentFee != feePerByte {
		return fmt.Errorf("Fee was not set")
	}

	return nil
}

// GetFee returns the currently configured fee in satoshi per byte for [coinType]
func (c *LitRpcClient) GetFee(coinType uint32) (int64, error) {
	args := new(litrpc.FeeArgs)
	args.CoinType = coinType
	reply := new(litrpc.FeeReply)
	err := c.rpcConn.Call("LitRPC.GetFee", args, reply)
	if err != nil {
		return 0, err
	}

	return reply.CurrentFee, nil
}

// GetAddresses returns a list of (newly generated or existing) addresses. Generates [numberToMake] addresses for
// coin type [coinType]. if [numberToMake] is 0, will return the existing addresses. Returns bech32 by default, or
// legacy addresses when you set [legacy] to true
func (c *LitRpcClient) GetAddresses(coinType, numberToMake uint32, legacy bool) ([]string, error) {
	args := new(litrpc.AddressArgs)
	args.CoinType = coinType
	args.NumToMake = numberToMake
	reply := new(litrpc.AddressReply)
	err := c.rpcConn.Call("LitRPC.Address", args, reply)
	if err != nil {
		return nil, err
	}
	if reply.LegacyAddresses == nil || reply.WitAddresses == nil {
		return nil, fmt.Errorf("Unexpected reply from server")
	}

	if legacy {
		return reply.LegacyAddresses, nil
	} else {
		return reply.WitAddresses, nil
	}
}

// ListChannels returns a list of channels (both active and closed)
func (c *LitRpcClient) ListChannels() ([]litrpc.ChannelInfo, error) {
	empty := make([]litrpc.ChannelInfo, 0)
	args := new(litrpc.NoArgs)

	reply := new(litrpc.ChannelListReply)
	err := c.rpcConn.Call("LitRPC.ChannelList", args, reply)
	if err != nil {
		return empty, err
	}
	if reply.Channels == nil {
		return empty, nil
	}

	return reply.Channels, nil
}

// FundChannel creates a new payment channel by funding a multi-sig output and exchanging the initial state
// between peers. After the channel exists, funds can freely be exchanged between peers without
// using the blockchain. Will create a channel of coin type [coinType] with peer [peerIndex]. It will fund it
// with [amount] from our wallet, and send over [initialSend] to our peer upon opening. If needed, [data] can
// be used to associate arbitrary data with the payment (like an invoice reference)
func (c *LitRpcClient) FundChannel(peerIndex, coinType uint32, amount, initialSend int64, data []byte) error {
	args := new(litrpc.FundArgs)
	args.Peer = peerIndex
	args.CoinType = coinType
	args.Capacity = amount
	args.InitialSend = initialSend
	copy(args.Data[:], data)
	reply := new(litrpc.StatusReply)
	err := c.rpcConn.Call("LitRPC.FundChannel", args, reply)
	if err != nil {
		return err
	}
	if strings.Index(reply.Status, "funded channel") == -1 {
		return fmt.Errorf("Unexpected response from server: %s", reply.Status)
	}

	return nil
}

// StateDump dumps all the known (previous) states to channels. This can be useful when
// analyzing payment references periodically. The data of each individual state
// is returned in the array of JusticeTx objects.
func (c *LitRpcClient) StateDump() ([]qln.JusticeTx, error) {
	empty := []qln.JusticeTx{}
	args := new(litrpc.NoArgs)

	reply := new(litrpc.StateDumpReply)
	err := c.rpcConn.Call("LitRPC.StateDump", args, reply)
	if err != nil {
		return empty, err
	}
	if reply.Txs == nil {
		return empty, nil
	}

	return reply.Txs, nil
}

// Push pushes [amount] satoshi through channel [channelIndex] to the other peer. If needed, you can use [data] to
// associate arbitrary data with the payment (like an invoice reference)
func (c *LitRpcClient) Push(channelIndex uint32, amount int64, data []byte) (uint64, error) {
	args := new(litrpc.PushArgs)
	args.ChanIdx = channelIndex
	args.Amt = amount
	copy(args.Data[:], data)
	reply := new(litrpc.PushReply)
	err := c.rpcConn.Call("LitRPC.Push", args, reply)
	if err != nil {
		return 0, err
	}
	return reply.StateIndex, nil
}

// Close collaboratively closes channel [channelIndex] and returns the funds to the wallet
func (c *LitRpcClient) CloseChannel(channelIndex uint32) error {
	args := new(litrpc.ChanArgs)
	args.ChanIdx = channelIndex
	reply := new(litrpc.StatusReply)
	err := c.rpcConn.Call("LitRPC.CloseChannel", args, reply)
	if err != nil {
		return err
	}
	if strings.Index(reply.Status, "OK closed") == -1 {
		return fmt.Errorf("Unexpected response from server: %s", reply.Status)
	}

	return nil
}

// Break breaks channel [channelIndex] and returns the funds to the wallet. This
// is an uncooperative closing, and might require some time for the funds to be
// returned to the wallet
func (c *LitRpcClient) BreakChannel(channelIndex uint32) error {
	args := new(litrpc.ChanArgs)
	args.ChanIdx = channelIndex
	reply := new(litrpc.StatusReply)
	err := c.rpcConn.Call("LitRPC.BreakChannel", args, reply)
	if err != nil {
		return err
	}
	if reply.Status == "" {
		return fmt.Errorf("Unexpected response from server")
	}

	return nil
}

// ImportOracle imports an oracle that exposes a REST API at [url], and saves it under display name [name]
func (c *LitRpcClient) ImportOracle(url, name string) (*dlc.DlcOracle, error) {
	args := new(litrpc.ImportOracleArgs)
	args.Url = url
	args.Name = name
	reply := new(litrpc.ImportOracleReply)
	err := c.rpcConn.Call("LitRPC.ImportOracle", args, reply)
	if err != nil {
		return nil, err
	}
	return reply.Oracle, nil
}

// AddOracle adds an oracle using its public key [pubkeyHex] (33 bytes hex), and saves it under display name [name]
func (c *LitRpcClient) AddOracle(pubKeyHex, name string) (*dlc.DlcOracle, error) {
	args := new(litrpc.AddOracleArgs)
	args.Key = pubKeyHex
	args.Name = name
	reply := new(litrpc.AddOracleReply)
	err := c.rpcConn.Call("LitRPC.AddOracle", args, reply)
	if err != nil {
		return nil, err
	}
	return reply.Oracle, nil
}

// ListOracles returns a list of all known oracles
func (c *LitRpcClient) ListOracles() ([]*dlc.DlcOracle, error) {
	empty := []*dlc.DlcOracle{}
	args := new(litrpc.NoArgs)

	reply := new(litrpc.ListOraclesReply)
	err := c.rpcConn.Call("LitRPC.ListOracles", args, reply)
	if err != nil {
		return empty, err
	}
	if reply.Oracles == nil {
		return empty, nil
	}

	return reply.Oracles, nil
}

// NewContract creates a new, empty draft contract and returns it
func (c *LitRpcClient) NewContract() (*lnutil.DlcContract, error) {
	args := new(litrpc.NoArgs)

	reply := new(litrpc.NewContractReply)
	err := c.rpcConn.Call("LitRPC.NewContract", args, reply)
	if err != nil {
		return nil, err
	}
	if reply.Contract == nil {
		return nil, fmt.Errorf("No contract returned from server")
	}

	return reply.Contract, nil
}

// GetContract returns the contract with id [contractIndex]
func (c *LitRpcClient) GetContract(contractIndex uint64) (*lnutil.DlcContract, error) {
	args := new(litrpc.GetContractArgs)
	args.Idx = contractIndex
	reply := new(litrpc.GetContractReply)
	err := c.rpcConn.Call("LitRPC.GetContract", args, reply)
	if err != nil {
		return nil, err
	}
	if reply.Contract == nil {
		return nil, fmt.Errorf("No contract returned from server")
	}

	return reply.Contract, nil
}

// ListContracts returns all known contracts
func (c *LitRpcClient) ListContracts() ([]*lnutil.DlcContract, error) {
	args := new(litrpc.NoArgs)

	reply := new(litrpc.ListContractsReply)
	err := c.rpcConn.Call("LitRPC.ListContracts", args, reply)
	if err != nil {
		return []*lnutil.DlcContract{}, err
	}
	if reply.Contracts == nil {
		return []*lnutil.DlcContract{}, fmt.Errorf("No contract returned from server")
	}

	return reply.Contracts, nil
}

// OfferContract offers contract [contractIndex] to peer [peerIndex]
func (c *LitRpcClient) OfferContract(contractIndex uint64, peerIndex uint32) error {
	args := new(litrpc.OfferContractArgs)
	args.CIdx = contractIndex
	args.PeerIdx = peerIndex
	reply := new(litrpc.OfferContractReply)
	err := c.rpcConn.Call("LitRPC.OfferContract", args, reply)
	if err != nil {
		return err
	}
	if !reply.Success {
		return fmt.Errorf("Server returned success = false")
	}

	return nil
}

// AcceptContract accepts the contract with id [contractIndex]
func (c *LitRpcClient) AcceptContract(contractIndex uint64) error {
	args := new(litrpc.AcceptContractArgs)
	args.CIdx = contractIndex
	reply := new(litrpc.AcceptContractReply)
	err := c.rpcConn.Call("LitRPC.AcceptContract", args, reply)
	if err != nil {
		return err
	}
	if !reply.Success {
		return fmt.Errorf("Server returned success = false")
	}

	return nil
}

// DeclineContract declines the contract with id [contractIndex]
func (c *LitRpcClient) DeclineContract(contractIndex uint64) error {
	args := new(litrpc.DeclineContractArgs)
	args.CIdx = contractIndex
	reply := new(litrpc.DeclineContractReply)
	err := c.rpcConn.Call("LitRPC.DeclineContract", args, reply)
	if err != nil {
		return err
	}
	if !reply.Success {
		return fmt.Errorf("Server returned success = false")
	}

	return nil
}

// SettleContract settles the contract with id [contractIndex] using
// oracle value [oracleValue] and signature [oracleSignature]
func (c *LitRpcClient) SettleContract(contractIndex uint64, oracleValue int64, oracleSignature []byte) error {
	args := new(litrpc.SettleContractArgs)
	args.CIdx = contractIndex
	copy(args.OracleSig[:], oracleSignature)
	args.OracleValue = oracleValue
	reply := new(litrpc.SettleContractReply)
	err := c.rpcConn.Call("LitRPC.SettleContract", args, reply)
	if err != nil {
		return err
	}
	if !reply.Success {
		return fmt.Errorf("Server returned success = false")
	}

	return nil
}

// SetContractDivision defines how the funds are divided based on the oracle's value, following a linear divison.
// When the oracle value is [valueFullyOurs], we get all the funds in the contract. When the value is [valueFullyTheirs]
// our counter party gets all the funds. Between those two, a linear division is followed
func (c *LitRpcClient) SetContractDivision(contractIndex uint64, valueFullyOurs, valueFullyTheirs int64) error {
	args := new(litrpc.SetContractDivisionArgs)
	args.CIdx = contractIndex
	args.ValueFullyOurs = valueFullyOurs
	args.ValueFullyTheirs = valueFullyTheirs
	reply := new(litrpc.SetContractDivisionReply)
	err := c.rpcConn.Call("LitRPC.SetContractDivision", args, reply)
	if err != nil {
		return err
	}
	if !reply.Success {
		return fmt.Errorf("Server returned success = false")
	}

	return nil
}

// SetContractCoinType specifies to use coin type [coinTyope] for the contract [contractIndex]. This cointype must be available or the server will return an error.
func (c *LitRpcClient) SetContractCoinType(contractIndex uint64, coinType uint32) error {
	args := new(litrpc.SetContractCoinTypeArgs)
	args.CIdx = contractIndex
	args.CoinType = coinType
	reply := new(litrpc.SetContractCoinTypeReply)
	err := c.rpcConn.Call("LitRPC.SetContractCoinType", args, reply)
	if err != nil {
		return err
	}
	if !reply.Success {
		return fmt.Errorf("Server returned success = false")
	}

	return nil
}

// SetContractFunding describes how the funding of the contract [contractIndex] is supposed to happen. It will make us
// fund [ourAmount] satoshi and request our counter party to fund [theirAmount] satoshi
func (c *LitRpcClient) SetContractFunding(contractIndex uint64, ourAmount, theirAmount int64) error {
	args := new(litrpc.SetContractFundingArgs)
	args.CIdx = contractIndex
	args.OurAmount = ourAmount
	args.TheirAmount = theirAmount
	reply := new(litrpc.SetContractFundingReply)
	err := c.rpcConn.Call("LitRPC.SetContractFunding", args, reply)
	if err != nil {
		return err
	}
	if !reply.Success {
		return fmt.Errorf("Server returned success = false")
	}

	return nil
}

// SetContractSettlementTime sets the time (unix timestamp) the contract [contractIndex] is supposed to settle to [settlementTime]
func (c *LitRpcClient) SetContractSettlementTime(contractIndex uint64, settlementTime uint64) error {
	args := new(litrpc.SetContractSettlementTimeArgs)
	args.CIdx = contractIndex
	args.Time = settlementTime
	reply := new(litrpc.SetContractSettlementTimeReply)
	err := c.rpcConn.Call("LitRPC.SetContractSettlementTime", args, reply)
	if err != nil {
		return err
	}
	if !reply.Success {
		return fmt.Errorf("Server returned success = false")
	}

	return nil
}

// SetContractRPoint sets the public key of the R-point [rPoint] the oracle will use to sign the message with that is used
// to settle contract [contractIndex]
func (c *LitRpcClient) SetContractRPoint(contractIndex uint64, rPoint []byte) error {
	args := new(litrpc.SetContractRPointArgs)
	args.CIdx = contractIndex
	copy(args.RPoint[:], rPoint)
	reply := new(litrpc.SetContractRPointReply)
	err := c.rpcConn.Call("LitRPC.SetContractRPoint", args, reply)
	if err != nil {
		return err
	}
	if !reply.Success {
		return fmt.Errorf("Server returned success = false")
	}

	return nil
}

// SetContractDatafeed sets a data feed by index to a contract, which is then
// used to fetch the R-point from the oracle's REST API
func (c *LitRpcClient) SetContractDatafeed(contractIndex uint64, feedIndex uint64) error {
	args := new(litrpc.SetContractDatafeedArgs)
	args.CIdx = contractIndex
	args.Feed = feedIndex
	reply := new(litrpc.SetContractDatafeedReply)
	err := c.rpcConn.Call("LitRPC.SetContractDatafeed", args, reply)
	if err != nil {
		return err
	}
	if !reply.Success {
		return fmt.Errorf("Server returned success = false")
	}

	return nil
}

// SetContractOracle configures contract [contractIndex] to use oracle with index [oracleIndex]. You need to import the oracle first.
func (c *LitRpcClient) SetContractOracle(contractIndex, oracleIndex uint64) error {
	args := new(litrpc.SetContractOracleArgs)
	args.CIdx = contractIndex
	args.OIdx = oracleIndex
	reply := new(litrpc.SetContractOracleReply)
	err := c.rpcConn.Call("LitRPC.SetContractOracle", args, reply)
	if err != nil {
		return err
	}
	if !reply.Success {
		return fmt.Errorf("Server returned success = false")
	}

	return nil
}
