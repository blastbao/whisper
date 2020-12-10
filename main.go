package main

import (
	"flag"
	"io/ioutil"

	"github.com/blastbao/whisper/agent"
	"github.com/blastbao/whisper/center"
	"github.com/blastbao/whisper/client"
	"github.com/blastbao/whisper/common"
	"github.com/blastbao/whisper/mediator"
	"github.com/valyala/gorpc"
)

func controlMediator(mediatorHost, controlBodyFile string) {
	cl := &mediator.NetClient{}
	if e := cl.Start(mediatorHost); e != nil {
		common.Log.Error("client start error", e)
		return
	}
	body, e := ioutil.ReadFile(controlBodyFile)
	if e != nil {
		common.Log.Error("read control body file error", e)
		return
	}
	cl.Send(mediator.Pack{Command: mediator.CMD_DO_NOTIFY, Body: body})
	cl.Close()
}

func closeMediator(mediatorHost string) {
	cl := &mediator.NetClient{}
	if e := cl.Start(mediatorHost); e != nil {
		common.Log.Error("client start error", e)
		return
	}
	cl.Send(mediator.Pack{Command: mediator.CMD_CLOSE})
	cl.Close()
}

func closeCenter(rpcHost string) {
	cl := gorpc.NewTCPClient(rpcHost)
	cl.Start()
	defer cl.Stop()

	cl.Call(mediator.Pack{Command: mediator.CMD_CLOSE})
}

func closeAgent(rpcHost string) {
	cl := gorpc.NewTCPClient(rpcHost)
	cl.Start()
	defer cl.Stop()

	cl.Call(mediator.Pack{Command: agent.AGENT_SERVER_COMMAND_CLOSE})
}

func closeClient(httpHost string) {
	common.Log.Info("not implement for closing client from shell, use mediator instead")
}

func main() {
	// 配置文件
	configFile := flag.String("configFile", "", "config file path")
	// 命令
	command := flag.String("command", "", "command(close or ...)")
	// 是否需要关闭
	isCloseCommand := "close" == *command
	//
	isMediatorControl := "mediatorControl" == *command

	if *configFile != "" {
		common.ConfFilePath = *configFile
	}

	// 读取配置文件
	c := common.GetConf()


	// Mediator
	if common.ROLE_MEDIATOR == c.Role {

		// 关闭 Mediator
		if isCloseCommand {
			closeMediator(c.MediatorHost)
			return
		// 发送 Notify
		} else if isMediatorControl {
			controlMediator(c.MediatorHost, c.MediatorControlBodyFile)
			return
		}

		// 其它 Command ，则启动 Mediator ，监听在 LOCALHOST:SERVER_PORT_MEDIATOR 地址上，数据目录为 c.BaseDir 。
		m := &mediator.Mediator{}
		m.Start(common.LOCALHOST, c.BaseDir)

		// 从 c.BaseDir 加载 m.BlockTree 索引。
		if e := m.Load(); e != nil {
			common.Log.Error("mediator load block error", e)
			return
		}

	// Center
	} else if common.ROLE_CENTER == c.Role {

		// 关闭 Center Svr
		if isCloseCommand {
			rpcHost := flag.String("rpcHost", "", "rpc host")
			closeCenter(*rpcHost)
			return
		}

		// 创建 Center ，并从 c.BaseDir 加载 oid => record 的索引。
		cc := &center.Center{}
		e := cc.Load(c.BaseDir)
		if e != nil {
			common.Log.Error("center load failed", e)
			return
		}

		// 启动 CenterServer，监听在 LOCALHOST:SERVER_PORT_CENTER 地址上
		s := &center.CenterServer{}
		s.Center = cc
		center.AddHandler2CenterServer(s)
		s.Start(c.MediatorHost, common.LOCALHOST)


	// Agent
	} else if common.ROLE_AGENT == c.Role {

		// 关闭 Agent
		if isCloseCommand {
			rpcHost := flag.String("rpcHost", "", "rpc host")
			closeAgent(*rpcHost)
			return
		}

		// 启动 node server
		s := &agent.NodeServer{}
		s.Start(c.MediatorHost, common.LOCALHOST)

	// Client
	} else if common.ROLE_CLIENT == c.Role {

		if isCloseCommand {
			httpHost := flag.String("httpHost", "", "http host")
			closeClient(*httpHost)
			return
		}

		// 创建 Client ，建立同 mediator server 建立长连接
		cl := &client.Client{}
		cl.Start(c.MediatorHost)
	} else {
		common.Log.Error("config file error, role required")
	}
}
