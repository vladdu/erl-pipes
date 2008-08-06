
-record(pipe, {
               name,
               inputs = [stdin],
               outputs = [stdout],
               context
              }).


-record(context, {
                   finished = false,
                   body,
                   inputs=[stdin],
                   state
                   }).

