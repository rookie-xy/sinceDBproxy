package utils

import (
    "github.com/rookie-xy/hubble/event"
    "github.com/rookie-xy/hubble/pipeline"
)

func Recall(events []event.Event, Q pipeline.Queue) error {
    for _, event := range events {
        if err := Q.Requeue(event); err != nil {
            return err
        }
    }

    return nil
}
