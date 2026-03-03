from unittest.mock import patch, MagicMock
from src.silver import run

@patch("src.silver.psycopg2.connect")
def test_silver_dq_pass(mock_connect):

    mock_conn = MagicMock()
    mock_connect.return_value = mock_conn

    mock_cursor = MagicMock()
    mock_conn.cursor.return_value = mock_cursor

    # Bronze retorna registros válidos
    mock_cursor.fetchall.return_value = [
        [{"id": "1"}]
    ]

    run()

    assert mock_conn.commit.called