"""
Implementation of Local Lambda runner
"""

import logging
from typing import Optional

from samcli.commands.local.lib.local_lambda import LocalLambdaRunner
from samcli.commands.local.lib.remote_lambda import RemoteLambdaRunner
from samcli.lib.utils.stream_writer import StreamWriter

LOG = logging.getLogger(__name__)


class LambdaRunner:

    def __init__(
        self,
        local_lambda_runner: LocalLambdaRunner,
        remote_lambda_runner: RemoteLambdaRunner,
    ) -> None:
        self.local_lambda_runner = local_lambda_runner
        self.remote_lambda_runner = remote_lambda_runner

    def invoke(
        self,
        function_identifier: str,
        event: str,
        stdout: Optional[StreamWriter] = None,
        stderr: Optional[StreamWriter] = None,
    ) -> None:
        """
        Find the Lambda function with given name and invoke it and check if it exists locally. If it exists
        locally, invoke it locally. If it does not exist locally, invoke it remotely.

        This function will block until either the function completes or times out.

        Parameters
        ----------
        function_identifier str
            Identifier of the Lambda function to invoke, it can be logicalID, function name or full path
        event str
            Event data passed to the function. Must be a valid JSON String.
        stdout samcli.lib.utils.stream_writer.StreamWriter
            Stream writer to write the output of the Lambda function to.
        stderr samcli.lib.utils.stream_writer.StreamWriter
            Stream writer to write the Lambda runtime logs to.

        Raises
        ------
        FunctionNotfound
            When we cannot find a function with the given name
        """

        # Generate the correct configuration based on given inputs
        function = self.local_lambda_runner.provider.get(function_identifier)

        if not function:
            self.remote_lambda_runner.invoke(function_identifier, event, stdout, stderr)
        else:
            self.local_lambda_runner.invoke(function_identifier, event, stdout, stderr)
