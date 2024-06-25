"""
Implementation of Local Lambda runner
"""

import base64
import logging
from typing import Optional

from samcli.commands.local.lib.debug_context import DebugContext
from samcli.lib.remote_invoke.lambda_invoke_executors import LambdaInvokeExecutor
from samcli.lib.remote_invoke.remote_invoke_executors import RemoteInvokeExecutionInfo, RemoteInvokeOutputFormat
from samcli.lib.utils.boto_utils import get_boto_client_provider_with_config
from samcli.lib.utils.stream_writer import StreamWriter

LOG = logging.getLogger(__name__)


class RemoteLambdaRunner:
    """
    Runs Lambda functions locally. This class is a wrapper around the `samcli.local` library which takes care
    of actually running the function on a Docker container.
    """

    MAX_DEBUG_TIMEOUT = 36000  # 10 hours in seconds
    WIN_ERROR_CODE = 1314

    def __init__(
        self,
        debug_context: Optional[DebugContext] = None,
        aws_profile: Optional[str] = None,
        aws_region: Optional[str] = None,
    ) -> None:
        """
        Initializes the class

        :param DebugContext debug_context: Optional. Debug context for the function (includes port, args, and path).
        :param string aws_profile: Optional. Name of the profile to fetch AWS credentials from.
        :param string aws_region: Optional. AWS Region to use.
        """

        self.aws_profile = aws_profile
        self.aws_region = aws_region
        self.debug_context = debug_context

    def invoke(
        self,
        function_identifier: str,
        event: str,
        stdout: Optional[StreamWriter] = None,
        stderr: Optional[StreamWriter] = None,
    ) -> None:
        """
        Find the Lambda function with given name and invoke it. Pass the given event to the function and return
        response through the given streams.

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

        boto_client_provider = get_boto_client_provider_with_config(
            region_name=self.aws_region, profile=self.aws_profile
        )
        output_format = RemoteInvokeOutputFormat.JSON
        remote_invoke_input = RemoteInvokeExecutionInfo(
            event,
            None,
            {},
            output_format,
        )

        lambda_client = boto_client_provider("lambda")
        remote_executor = LambdaInvokeExecutor(lambda_client, function_identifier, output_format)
        remote_execution_info = remote_executor.execute(remote_invoke_input)

        remote_invoke_response = next(remote_execution_info) # type: ignore

        if remote_invoke_response is None:
            if stderr:
                stderr.write_str("Remote invoke failed.")
            raise Exception(
                "Remote invoke failed to execute. remote_executor did not return any response when iterating"
            )

        response = remote_invoke_response.response
        response_payload = response["Payload"].read()

        print("RESPONSE PAYLOAD", response_payload)

        if remote_invoke_response.error:
            if stderr:
                stderr.write_str("Remote invoke failed")
                stderr.write_bytes(response_payload)
            if self.is_debugging():
                LOG.debug("Remote invoke failed:")
                LOG.debug(base64.b64decode(response["LogResult"]))
            raise Exception(remote_invoke_response.error)
        elif stdout:
            stdout.write_bytes(response_payload)

    def is_debugging(self) -> bool:
        """
        Are we debugging the invoke?

        Returns
        -------
        bool
            True, if we are debugging the invoke ie. the Docker container will break into the debugger and wait for
            attach
        """
        return bool(self.debug_context)
