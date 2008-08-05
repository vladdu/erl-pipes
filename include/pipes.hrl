
-record(pipe, {
               name,
               inputs = [default],
               outputs = [default],
               context
              }).


-record(context, {
                   finished = false,
                   body,
                   inputs=[default],
                   state
                   }).

