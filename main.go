package main

import (
    "github.com/rookie-xy/hubble/command"
    "github.com/rookie-xy/hubble/module"
    "github.com/rookie-xy/hubble/log"
    "github.com/rookie-xy/hubble/register"
    "github.com/rookie-xy/hubble/factory"
    "github.com/rookie-xy/hubble/pipeline"
    "github.com/rookie-xy/hubble/plugin"
    "github.com/rookie-xy/hubble/adapter"
    "github.com/rookie-xy/modules/proxy/sinceDB/utils"
  . "github.com/rookie-xy/hubble/log/level"
)

const Name  = "sinceDB"

type sincedb struct {
    log.Log
    level     Level

    name      string

    pipeline  pipeline.Queue
    client    adapter.SinceDB
    done      chan struct{}

    batch     int
}

var (
    Pipeline  = command.New( plugin.Flag, "pipeline.channel",  nil, "This option use to group" )
    batch     = command.New( module.Flag, "batch",    64, "This option use to group" )
    client    = command.New( plugin.Flag, "client.sinceDB",    nil, "This option use to group" )
)

var commands = []command.Item{

    { Pipeline,
      command.FILE,
      module.Proxys,
      Name,
      command.SetObject,
      nil },

    { batch,
      command.FILE,
      module.Proxys,
      Name,
      command.SetObject,
      nil },

    { client,
      command.FILE,
      module.Proxys,
      Name,
      command.SetObject,
      nil },

}

func New(log log.Log) module.Template {
    return &sincedb{
        Log:  log,
        level: adapter.ToLevelLog(log).Get(),
        done: make(chan struct{}),
    }
}

func (s *sincedb) Init() {
    if key, ok := plugin.Name(Pipeline.GetKey()); ok {
        pipeline, err := factory.Pipeline(key, s.Log, Pipeline.GetValue())
        if err != nil {
            s.log(ERROR, Name+"; pipeline error %s", err)
            return
        } else {
            s.pipeline = pipeline
        }
    }
    s.name = client.GetKey()

    register.Queue(client.GetKey(), s.pipeline)

    if key, ok := plugin.Name(client.GetKey()); ok {
        if client, err := factory.Client(key, s.Log, client.GetValue()); err != nil {
            s.log(ERROR, Name + "; client error ", err)
            return
        } else {
            s.client = adapter.FileSinceDB(client)
            register.Forword(key, client)
        }
    }

    if value := batch.GetValue(); value != nil {
        if batch, err := value.GetInt(); err != nil {
            s.batch = batch
        }
    }
}

func (s *sincedb) Main() {
	s.log(INFO, Name +"; run component for %s", s.name)

    defer close(s.done)

    for {
        events, err := s.pipeline.Dequeues(s.batch)
        switch err {
        case pipeline.ErrClosed:
        	s.log(INFO, Name +"; close for %s, %s", s.name, pipeline.ErrClosed)
            return

        case pipeline.ErrEmpty:
            s.log(INFO, Name +"; empty for %s, %s", s.name, pipeline.ErrEmpty)
        default:
            s.log(WARN, Name +"; unknown queue event")
        }

        if events != nil {
            // why? event is nil?
            if err := s.client.Senders(events); err != nil {
                if err := utils.Recall(events, s.pipeline); err != nil {
                    s.log(ERROR, Name +"; recall error: %s", err)
                    return
                }
            }
        }
    }
}

func (s *sincedb) Exit(code int) {
    defer func() {
        <-s.done
        s.log(DEBUG,"%s component have exit", Name)
    }()

    s.log(INFO,"Exit component for %s", Name)
    s.pipeline.Close()
	s.client.Close()
}

func (s *sincedb) log(l Level, fmt string, args ...interface{}) {
    log.Print(s.Log, s.level, l, fmt, args...)
}

func init() {
    register.Component(module.Proxys, Name, commands, New)
}
