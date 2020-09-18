package main

import (
	"agent"
	"center"
	"client"
	"common"
	"flag"
	"github.com/valyala/gorpc"
	"io/ioutil"
	"mediator"
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
	configFile := flag.String("configFile", "", "config file path")
	command := flag.String("command", "", "command(close or ...)")
	isCloseCommand := "close" == *command
	isMediatorControl := "mediatorControl" == *command

	if *configFile != "" {
		common.ConfFilePath = *configFile
	}
	c := common.GetConf()

	if common.ROLE_MEDIATOR == c.Role {
		if isCloseCommand {
			closeMediator(c.MediatorHost)
			return
		} else if isMediatorControl {
			controlMediator(c.MediatorHost, c.MediatorControlBodyFile)
			return
		}

		m := &mediator.Mediator{}
		m.Start(common.LOCALHOST, c.BaseDir)
		if e := m.Load(); e != nil {
			common.Log.Error("mediator load block error", e)
			return
		}
	} else if common.ROLE_CENTER == c.Role {
		if isCloseCommand {
			rpcHost := flag.String("rpcHost", "", "rpc host")
			closeCenter(*rpcHost)
			return
		}

		cc := &center.Center{}
		e := cc.Load(c.BaseDir)
		if e != nil {
			common.Log.Error("center load failed", e)
			return
		}

		s := &center.CenterServer{}
		s.Center = cc
		center.AddHandler2CenterServer(s)
		s.Start(c.MediatorHost, common.LOCALHOST)
	} else if common.ROLE_AGENT == c.Role {
		if isCloseCommand {
			rpcHost := flag.String("rpcHost", "", "rpc host")
			closeAgent(*rpcHost)
			return
		}

		// node server start
		s := &agent.NodeServer{}
		s.Start(c.MediatorHost, common.LOCALHOST)
	} else if common.ROLE_CLIENT == c.Role {
		if isCloseCommand {
			httpHost := flag.String("httpHost", "", "http host")
			closeClient(*httpHost)
			return
		}

		cl := &client.Client{}
		cl.Start(c.MediatorHost)
	} else {
		common.Log.Error("config file error, role required")
	}
}
