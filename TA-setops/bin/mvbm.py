#!/usr/bin/env python

from splunklib.searchcommands import dispatch, StreamingCommand, Configuration, Option, validators
import sys

@Configuration()
class MVBMCommand(StreamingCommand):
    """ 

    ##Syntax


    ##Description


    ##Example


    """
    field = Option(
        doc='''
        **Syntax:** **field=<fieldname>*
        **Description:** ''',
        require=True)

    def stream(self, events):

        for event in events:

            if self.field in event:

                if len(event[self.field]) > 0:

                    for value in event[self.field]:
                        event[self.field + "_mvbm_" + value] = 1

            yield event

dispatch(MVBMCommand, sys.argv, sys.stdin, sys.stdout, __name__)
