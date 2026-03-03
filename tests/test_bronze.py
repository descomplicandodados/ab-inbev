from unittest.mock import patch, MagicMock
from src.bronze import run

@patch("src.bronze.requests.get")
@patch("src.bronze.psycopg2.connect")
def test_bronze_run(mock_connect, mock_get):

    mock_conn = MagicMock()
    mock_connect.return_value = mock_conn

    mock_get.return_value.status_code = 200
    mock_get.return_value.json.side_effect = [
        [{"id": "1"}],
        []
    ]

    run()

    assert mock_conn.cursor.called