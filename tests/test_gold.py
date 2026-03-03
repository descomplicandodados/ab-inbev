from unittest.mock import patch, MagicMock
from src.gold import run

@patch("src.gold.psycopg2.connect")
def test_gold_run(mock_connect):

    mock_conn = MagicMock()
    mock_connect.return_value = mock_conn

    run()

    assert mock_conn.commit.called