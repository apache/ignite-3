package org.apache.ignite.cli.core.call;

/**
 * Call that represents an action that can be performed given an input.
 * It can be rest call, dictionary lookup or whatever.
 *
 * @param <IT> Input for the call.
 * @param <OT> Output of the call.
 */
public interface Call<IT extends CallInput, OT> {
    CallOutput<OT> execute(IT input);
}
