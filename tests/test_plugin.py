from unittest.mock import Mock

import pytest
from litestar.cli._utils import LitestarGroup as Group

from litestar_saq.config import SAQConfig, TaskQueues
from litestar_saq.plugin import SAQPlugin

# Assuming SAQConfig, Worker, Group, AppConfig, TaskQueues, Queue are available and can be imported
# Assuming there are meaningful __eq__ methods for comparisons where needed


# Test on_cli_init method
@pytest.mark.parametrize(
    "cli_group",
    [
        # ID: Test-CLI-Init-1
        Mock(Group),
    ],
    ids=["happy-path"],
)
def test_on_cli_init(cli_group: Group) -> None:
    # Arrange
    config = Mock(SAQConfig)
    plugin = SAQPlugin(config)

    # Act
    plugin.on_cli_init(cli_group)

    # Assert
    cli_group.add_command.assert_called_once()


# Test get_queues method
@pytest.mark.parametrize(
    "queues",
    [
        # ID: Test-Get-Queues-1
        Mock(),
    ],
    ids=["happy-path"],
)
def test_get_queues(queues: TaskQueues) -> None:
    # Arrange
    config = Mock(SAQConfig, get_queues=Mock(return_value=queues))
    plugin = SAQPlugin(config)

    # Act
    result = plugin.get_queues()

    # Assert
    assert result == queues
