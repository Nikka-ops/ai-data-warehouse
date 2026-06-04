# -*- coding: utf-8 -*-
"""Comprehensive unit tests for ai_layer modules: nl2sql, rag_engine, agents.

All LLM and external service calls are mocked — no real API calls are made.
Imports are wrapped in try/except so tests skip gracefully when modules are
unavailable (pandas, chromadb, langchain_openai, etc. may not be installed in
all environments).
"""
import pytest
from unittest.mock import MagicMock, patch

# ── Optional: pandas (may not be installed) ───────────────────────────────────
try:
    import pandas as pd
    _pandas_available = True
except ImportError:
    pd = None  # type: ignore[assignment]
    _pandas_available = False

# ── nl2sql imports ────────────────────────────────────────────────────────────
try:
    from ai_layer.nl2sql import (
        _clean_sql,
        _format_nl2sql_history,
        _make_result_summary,
        generate_sql,
        _explain_sql,
        _repair_sql,
        _score_insight,
        _generate_valid_sql,
        nl2sql,
    )
    _nl2sql_available = True
except Exception:
    _nl2sql_available = False

# ── rag_engine imports ────────────────────────────────────────────────────────
try:
    from ai_layer.rag_engine import (
        split_chunks,
        _build_retrieval_query,
        _batch_score_relevance,
        _rewrite_query,
        _score_groundedness,
        retrieve,
        route_question,
        rag_query,
    )
    _rag_available = True
except Exception:
    _rag_available = False

# ── agents imports ────────────────────────────────────────────────────────────
try:
    from ai_layer.agents import (
        AgentState,
        supervisor_node,
        route_supervisor,
        synthesize_node,
        _run,
        run_free_agent,
    )
    _agents_available = True
except Exception:
    _agents_available = False


# ══════════════════════════════════════════════════════════════════════════════
# Helpers
# ══════════════════════════════════════════════════════════════════════════════

def _make_llm_response(content: str) -> MagicMock:
    """Return a mock object shaped like an openai ChatCompletion response."""
    choice = MagicMock()
    choice.message.content = content
    resp = MagicMock()
    resp.choices = [choice]
    return resp


def _make_df(data: dict):
    """Return a real DataFrame if pandas is available, else a MagicMock that
    behaves enough like a DataFrame for the nl2sql tests."""
    if _pandas_available:
        return pd.DataFrame(data)
    mock_df = MagicMock()
    # Simulate the attributes nl2sql code actually reads
    mock_df.empty = False
    mock_df.__len__ = MagicMock(return_value=len(list(data.values())[0]))
    cols = list(data.keys())
    mock_df.select_dtypes.return_value.columns.tolist.return_value = cols
    # For each numeric column, simulate min/max
    for col, values in data.items():
        getattr(mock_df, col, None)
    mock_df.__getitem__ = MagicMock(side_effect=lambda c: MagicMock(
        min=MagicMock(return_value=min(data[c])),
        max=MagicMock(return_value=max(data[c])),
    ))
    mock_df.head.return_value.to_markdown.return_value = str(data)
    return mock_df


# ══════════════════════════════════════════════════════════════════════════════
# nl2sql — _clean_sql
# ══════════════════════════════════════════════════════════════════════════════

@pytest.mark.skipif(not _nl2sql_available, reason="ai_layer.nl2sql not importable")
class TestCleanSql:
    def test_strips_sql_fence_with_language_tag(self):
        raw = "```sql\nSELECT 1\n```"
        assert _clean_sql(raw) == "SELECT 1"

    def test_strips_plain_fence(self):
        raw = "```\nSELECT 2\n```"
        assert _clean_sql(raw) == "SELECT 2"

    def test_strips_trailing_semicolon(self):
        assert _clean_sql("SELECT 3;") == "SELECT 3"

    def test_no_fence_no_semicolon_unchanged(self):
        assert _clean_sql("SELECT a FROM t") == "SELECT a FROM t"

    def test_whitespace_trimmed(self):
        assert _clean_sql("   SELECT 4   ") == "SELECT 4"

    def test_case_insensitive_sql_fence(self):
        raw = "```SQL\nSELECT 5\n```"
        assert _clean_sql(raw) == "SELECT 5"


# ══════════════════════════════════════════════════════════════════════════════
# nl2sql — _format_nl2sql_history
# ══════════════════════════════════════════════════════════════════════════════

@pytest.mark.skipif(not _nl2sql_available, reason="ai_layer.nl2sql not importable")
class TestFormatNl2SqlHistory:
    def test_empty_list_returns_empty_string(self):
        assert _format_nl2sql_history([]) == ""

    def test_three_turn_history_includes_all_fields(self):
        history = [
            {"question": "Q1", "sql": "SELECT 1", "result_summary": "1 行"},
            {"question": "Q2", "sql": "SELECT 2", "result_summary": ""},
            {"question": "Q3", "sql": "SELECT 3", "result_summary": "3 行"},
        ]
        result = _format_nl2sql_history(history)
        assert "Q1" in result
        assert "SELECT 1" in result
        assert "1 行" in result
        assert "Q3" in result
        assert "【对话历史" in result

    def test_missing_result_summary_omits_line(self):
        history = [{"question": "Q", "sql": "SELECT 1"}]
        result = _format_nl2sql_history(history)
        assert "结果摘要" not in result

    def test_keeps_at_most_three_turns(self):
        history = [
            {"question": f"Q{i}", "sql": f"SELECT {i}", "result_summary": ""}
            for i in range(6)
        ]
        result = _format_nl2sql_history(history)
        # Only turns 3, 4, 5 should appear (last 3)
        assert "Q5" in result
        assert "Q3" in result
        assert "SELECT 0" not in result


# ══════════════════════════════════════════════════════════════════════════════
# nl2sql — _make_result_summary
# ══════════════════════════════════════════════════════════════════════════════

@pytest.mark.skipif(
    not _nl2sql_available or not _pandas_available,
    reason="ai_layer.nl2sql or pandas not importable",
)
class TestMakeResultSummary:
    def test_empty_dataframe_returns_empty_message(self):
        assert _make_result_summary(pd.DataFrame()) == "结果为空"

    def test_non_empty_dataframe_includes_row_count(self):
        df = pd.DataFrame({"a": [1, 2, 3]})
        summary = _make_result_summary(df)
        assert "3 行" in summary

    def test_numeric_range_included(self):
        df = pd.DataFrame({"price": [10.0, 20.0, 30.0]})
        summary = _make_result_summary(df)
        assert "10.0" in summary
        assert "30.0" in summary

    def test_non_numeric_columns_excluded_from_range(self):
        df = pd.DataFrame({"name": ["alice", "bob"], "score": [90, 80]})
        summary = _make_result_summary(df)
        assert "score" in summary
        # "name" is a string column — should not appear as a range
        assert "name" not in summary


# ══════════════════════════════════════════════════════════════════════════════
# nl2sql — generate_sql
# ══════════════════════════════════════════════════════════════════════════════

@pytest.mark.skipif(not _nl2sql_available, reason="ai_layer.nl2sql not importable")
class TestGenerateSql:
    @patch("ai_layer.nl2sql.llm")
    def test_returns_cleaned_sql_from_llm(self, mock_llm):
        mock_llm.chat.completions.create.return_value = _make_llm_response(
            "```sql\nSELECT count() FROM dws.realtime_minute_stats\n```"
        )
        result = generate_sql("今日订单数", schema="some schema")
        assert result == "SELECT count() FROM dws.realtime_minute_stats"

    @patch("ai_layer.nl2sql.llm")
    def test_passes_history_in_system_prompt(self, mock_llm):
        mock_llm.chat.completions.create.return_value = _make_llm_response("SELECT 1")
        history = [{"question": "老问题", "sql": "SELECT 0", "result_summary": ""}]
        generate_sql("追问", schema="schema", history=history)
        call_kwargs = mock_llm.chat.completions.create.call_args[1]
        system_content = call_kwargs["messages"][0]["content"]
        assert "老问题" in system_content


# ══════════════════════════════════════════════════════════════════════════════
# nl2sql — _explain_sql
# ══════════════════════════════════════════════════════════════════════════════

@pytest.mark.skipif(not _nl2sql_available, reason="ai_layer.nl2sql not importable")
class TestExplainSql:
    def test_success_path_returns_true_empty_error(self):
        mock_ch = MagicMock()
        mock_ch.command.return_value = None
        ok, err = _explain_sql(mock_ch, "SELECT 1")
        assert ok is True
        assert err == ""

    def test_exception_returns_false_with_error_string(self):
        mock_ch = MagicMock()
        mock_ch.command.side_effect = RuntimeError("syntax error near 'SELCT'")
        ok, err = _explain_sql(mock_ch, "SELCT 1")
        assert ok is False
        assert "syntax error" in err


# ══════════════════════════════════════════════════════════════════════════════
# nl2sql — _repair_sql
# ══════════════════════════════════════════════════════════════════════════════

@pytest.mark.skipif(not _nl2sql_available, reason="ai_layer.nl2sql not importable")
class TestRepairSql:
    @patch("ai_layer.nl2sql.llm")
    def test_returns_cleaned_repaired_sql(self, mock_llm):
        mock_llm.chat.completions.create.return_value = _make_llm_response(
            "SELECT order_id FROM dws.realtime_minute_stats LIMIT 10"
        )
        result = _repair_sql("question", "bad sql", "error msg", "schema")
        assert result == "SELECT order_id FROM dws.realtime_minute_stats LIMIT 10"

    @patch("ai_layer.nl2sql.llm")
    def test_returns_original_sql_on_llm_failure(self, mock_llm):
        mock_llm.chat.completions.create.side_effect = RuntimeError("API down")
        result = _repair_sql("q", "SELECT bad", "err", "schema")
        assert result == "SELECT bad"


# ══════════════════════════════════════════════════════════════════════════════
# nl2sql — _score_insight
# ══════════════════════════════════════════════════════════════════════════════

@pytest.mark.skipif(not _nl2sql_available, reason="ai_layer.nl2sql not importable")
class TestScoreInsight:
    @patch("ai_layer.nl2sql.llm")
    def test_returns_parsed_float(self, mock_llm):
        mock_llm.chat.completions.create.return_value = _make_llm_response("0.85")
        score = _score_insight("good insight", "3 行，price 范围 10.0~30.0")
        assert abs(score - 0.85) < 1e-9

    @patch("ai_layer.nl2sql.llm")
    def test_llm_failure_returns_0_5(self, mock_llm):
        mock_llm.chat.completions.create.side_effect = Exception("timeout")
        score = _score_insight("some insight", "summary")
        assert score == 0.5

    @patch("ai_layer.nl2sql.llm")
    def test_score_clamped_at_1_0(self, mock_llm):
        mock_llm.chat.completions.create.return_value = _make_llm_response("1.5")
        score = _score_insight("insight", "summary")
        assert score <= 1.0

    @patch("ai_layer.nl2sql.llm")
    def test_score_clamped_at_0_0(self, mock_llm):
        mock_llm.chat.completions.create.return_value = _make_llm_response("-0.3")
        score = _score_insight("insight", "summary")
        assert score >= 0.0


# ══════════════════════════════════════════════════════════════════════════════
# nl2sql — _generate_valid_sql
# ══════════════════════════════════════════════════════════════════════════════

@pytest.mark.skipif(not _nl2sql_available, reason="ai_layer.nl2sql not importable")
class TestGenerateValidSql:
    @patch("ai_layer.nl2sql.generate_sql")
    @patch("ai_layer.nl2sql.validate_sql")
    @patch("ai_layer.nl2sql._explain_sql")
    def test_success_zero_repairs(self, mock_explain, mock_validate, mock_gen):
        mock_gen.return_value = "SELECT 1"
        mock_validate.return_value = None
        mock_explain.return_value = (True, "")
        sql, repairs, err = _generate_valid_sql(MagicMock(), "question", "schema", None)
        assert sql == "SELECT 1"
        assert repairs == 0
        assert err is None

    @patch("ai_layer.nl2sql.generate_sql")
    @patch("ai_layer.nl2sql.validate_sql")
    @patch("ai_layer.nl2sql._explain_sql")
    @patch("ai_layer.nl2sql._repair_sql")
    def test_single_repair_path(self, mock_repair, mock_explain, mock_validate, mock_gen):
        mock_gen.return_value = "SELECT bad"
        mock_validate.return_value = None
        mock_explain.side_effect = [(False, "bad syntax"), (True, "")]
        mock_repair.return_value = "SELECT fixed"
        sql, repairs, err = _generate_valid_sql(MagicMock(), "q", "schema", None)
        assert repairs == 1
        assert err is None
        assert sql == "SELECT fixed"

    @patch("ai_layer.nl2sql.generate_sql")
    @patch("ai_layer.nl2sql.validate_sql")
    @patch("ai_layer.nl2sql._explain_sql")
    @patch("ai_layer.nl2sql._repair_sql")
    def test_max_repair_exceeded_returns_error_message(
        self, mock_repair, mock_explain, mock_validate, mock_gen
    ):
        mock_gen.return_value = "SELECT bad"
        mock_validate.return_value = None
        mock_explain.return_value = (False, "persistent error")
        mock_repair.return_value = "SELECT still_bad"
        sql, repairs, err = _generate_valid_sql(MagicMock(), "q", "schema", None)
        assert repairs == 2
        assert err is not None
        assert "SQL 验证失败" in err


# ══════════════════════════════════════════════════════════════════════════════
# nl2sql — nl2sql (main entry point)
# ══════════════════════════════════════════════════════════════════════════════

@pytest.mark.skipif(not _nl2sql_available, reason="ai_layer.nl2sql not importable")
class TestNl2sql:
    @patch("ai_layer.nl2sql.get_ch_client")
    @patch("ai_layer.nl2sql.get_schema")
    @patch("ai_layer.nl2sql._generate_valid_sql")
    @patch("ai_layer.nl2sql._generate_insight_with_selfrag")
    def test_success_path_returns_expected_keys(
        self, mock_insight, mock_gen_sql, mock_schema, mock_ch
    ):
        mock_client = MagicMock()
        mock_ch.return_value = mock_client
        mock_schema.return_value = "schema text"
        mock_gen_sql.return_value = ("SELECT 1", 0, None)
        # Use a mock DataFrame so we don't need pandas
        mock_df = MagicMock()
        mock_df.__len__ = MagicMock(return_value=3)
        mock_df.empty = False
        mock_client.query_df.return_value = mock_df
        mock_insight.return_value = ("洞察内容", 0.9)

        result = nl2sql("今日 GMV")
        assert "sql" in result
        assert "data" in result
        assert "insight" in result
        assert "repair_attempts" in result
        assert result["sql"] == "SELECT 1"
        assert result["repair_attempts"] == 0
        assert result["error"] is None

    @patch("ai_layer.nl2sql.get_ch_client")
    @patch("ai_layer.nl2sql.get_schema")
    @patch("ai_layer.nl2sql._generate_valid_sql")
    def test_error_path_when_generate_valid_sql_raises(
        self, mock_gen_sql, mock_schema, mock_ch
    ):
        mock_ch.return_value = MagicMock()
        mock_schema.return_value = "schema"
        mock_gen_sql.side_effect = RuntimeError("LLM unavailable")
        result = nl2sql("问题")
        assert result["error"] is not None
        assert "LLM unavailable" in result["error"]

    @patch("ai_layer.nl2sql.get_ch_client")
    @patch("ai_layer.nl2sql.get_schema")
    @patch("ai_layer.nl2sql._generate_valid_sql")
    def test_validation_error_stops_before_query(
        self, mock_gen_sql, mock_schema, mock_ch
    ):
        mock_client = MagicMock()
        mock_ch.return_value = mock_client
        mock_schema.return_value = "schema"
        mock_gen_sql.return_value = ("SELECT bad", 2, "SQL 验证失败（已尝试修复 2 次）。")

        result = nl2sql("问题")
        assert result["error"] is not None
        assert result["sql"] == "SELECT bad"
        mock_client.query_df.assert_not_called()


# ══════════════════════════════════════════════════════════════════════════════
# rag_engine — split_chunks
# ══════════════════════════════════════════════════════════════════════════════

@pytest.mark.skipif(not _rag_available, reason="ai_layer.rag_engine not importable")
class TestSplitChunks:
    def test_short_text_stays_as_one_chunk(self):
        text = "短文本内容"
        chunks = split_chunks(text, "test.md")
        assert len(chunks) == 1
        assert chunks[0]["text"] == text
        assert chunks[0]["source"] == "test.md"

    def test_long_text_splits_into_multiple_chunks(self):
        from config import cfg
        # Two paragraphs whose combined size exceeds chunk_size
        para = "A" * (cfg.chunk_size // 2 + 10)
        text = para + "\n\n" + para + "\n\n" + para
        chunks = split_chunks(text, "src.md")
        assert len(chunks) >= 2

    def test_source_propagated_to_all_chunks(self):
        text = "Para one.\n\nPara two.\n\nPara three."
        chunks = split_chunks(text, "source_file.md")
        for chunk in chunks:
            assert chunk["source"] == "source_file.md"

    def test_empty_text_returns_no_chunks(self):
        chunks = split_chunks("", "empty.md")
        assert chunks == []

    def test_very_long_paragraph_generates_sub_chunks(self):
        from config import cfg
        big_para = "X" * (cfg.chunk_size + 50)
        chunks = split_chunks(big_para, "big.md")
        # A paragraph larger than chunk_size is split into sub-chunks
        assert len(chunks) >= 2


# ══════════════════════════════════════════════════════════════════════════════
# rag_engine — _build_retrieval_query
# ══════════════════════════════════════════════════════════════════════════════

@pytest.mark.skipif(not _rag_available, reason="ai_layer.rag_engine not importable")
class TestBuildRetrievalQuery:
    def test_no_history_returns_question_unchanged(self):
        result = _build_retrieval_query("what is GMV?", None)
        assert result == "what is GMV?"

    def test_with_history_prepends_context(self):
        history = [{"question": "上一个问题", "answer": "上一个答案"}]
        result = _build_retrieval_query("新问题", history)
        assert "新问题" in result
        assert "上一个问题" in result

    def test_empty_history_list_returns_question(self):
        result = _build_retrieval_query("my question", [])
        assert result == "my question"


# ══════════════════════════════════════════════════════════════════════════════
# rag_engine — _batch_score_relevance
# ══════════════════════════════════════════════════════════════════════════════

@pytest.mark.skipif(not _rag_available, reason="ai_layer.rag_engine not importable")
class TestBatchScoreRelevance:
    @patch("ai_layer.rag_engine.llm")
    def test_returns_parsed_float_scores(self, mock_llm):
        mock_llm.chat.completions.create.return_value = _make_llm_response("[0.9, 0.3]")
        chunks = [{"text": "chunk one"}, {"text": "chunk two"}]
        scores = _batch_score_relevance("question", chunks)
        assert len(scores) == 2
        assert abs(scores[0] - 0.9) < 1e-9
        assert abs(scores[1] - 0.3) < 1e-9

    @patch("ai_layer.rag_engine.llm")
    def test_llm_failure_returns_0_5_per_chunk(self, mock_llm):
        mock_llm.chat.completions.create.side_effect = RuntimeError("fail")
        chunks = [{"text": "a"}, {"text": "b"}]
        scores = _batch_score_relevance("q", chunks)
        assert scores == [0.5, 0.5]

    def test_empty_chunks_returns_empty_list(self):
        scores = _batch_score_relevance("q", [])
        assert scores == []

    @patch("ai_layer.rag_engine.llm")
    def test_scores_clamped_between_0_and_1(self, mock_llm):
        mock_llm.chat.completions.create.return_value = _make_llm_response("[1.5, -0.2]")
        chunks = [{"text": "a"}, {"text": "b"}]
        scores = _batch_score_relevance("q", chunks)
        assert all(0.0 <= s <= 1.0 for s in scores)


# ══════════════════════════════════════════════════════════════════════════════
# rag_engine — _rewrite_query
# ══════════════════════════════════════════════════════════════════════════════

@pytest.mark.skipif(not _rag_available, reason="ai_layer.rag_engine not importable")
class TestRewriteQuery:
    @patch("ai_layer.rag_engine.llm")
    def test_returns_rewritten_question(self, mock_llm):
        mock_llm.chat.completions.create.return_value = _make_llm_response("改写后的问题")
        result = _rewrite_query("原始问题")
        assert result == "改写后的问题"

    @patch("ai_layer.rag_engine.llm")
    def test_failure_returns_original_question(self, mock_llm):
        mock_llm.chat.completions.create.side_effect = RuntimeError("api error")
        result = _rewrite_query("原始问题")
        assert result == "原始问题"


# ══════════════════════════════════════════════════════════════════════════════
# rag_engine — _score_groundedness
# ══════════════════════════════════════════════════════════════════════════════

@pytest.mark.skipif(not _rag_available, reason="ai_layer.rag_engine not importable")
class TestScoreGroundedness:
    @patch("ai_layer.rag_engine.llm")
    def test_returns_parsed_float(self, mock_llm):
        mock_llm.chat.completions.create.return_value = _make_llm_response("0.8")
        score = _score_groundedness("answer text", "docs context")
        assert abs(score - 0.8) < 1e-9

    @patch("ai_layer.rag_engine.llm")
    def test_llm_failure_returns_0_5(self, mock_llm):
        mock_llm.chat.completions.create.side_effect = Exception("fail")
        score = _score_groundedness("answer", "docs")
        assert score == 0.5


# ══════════════════════════════════════════════════════════════════════════════
# rag_engine — retrieve
# ══════════════════════════════════════════════════════════════════════════════

@pytest.mark.skipif(not _rag_available, reason="ai_layer.rag_engine not importable")
class TestRetrieve:
    @patch("ai_layer.rag_engine.get_collection")
    def test_result_structure_has_required_keys(self, mock_get_col):
        mock_col = MagicMock()
        mock_col.count.return_value = 2
        mock_col.query.return_value = {
            "documents": [["chunk text A", "chunk text B"]],
            "metadatas": [[{"source": "file_a.md"}, {"source": "file_b.md"}]],
            "distances": [[0.1, 0.3]],
        }
        mock_get_col.return_value = mock_col

        results = retrieve("test question", top_k=2)
        assert len(results) == 2
        for item in results:
            assert "text" in item
            assert "source" in item
            assert "distance" in item

    @patch("ai_layer.rag_engine.get_collection")
    def test_values_match_query_output(self, mock_get_col):
        mock_col = MagicMock()
        mock_col.count.return_value = 1
        mock_col.query.return_value = {
            "documents": [["some text"]],
            "metadatas": [[{"source": "doc.md"}]],
            "distances": [[0.42]],
        }
        mock_get_col.return_value = mock_col

        results = retrieve("q", top_k=1)
        assert abs(results[0]["distance"] - 0.42) < 1e-9
        assert results[0]["source"] == "doc.md"
        assert results[0]["text"] == "some text"


# ══════════════════════════════════════════════════════════════════════════════
# rag_engine — route_question
# ══════════════════════════════════════════════════════════════════════════════

@pytest.mark.skipif(not _rag_available, reason="ai_layer.rag_engine not importable")
class TestRouteQuestion:
    @patch("ai_layer.rag_engine.llm")
    def test_b_response_returns_rag(self, mock_llm):
        mock_llm.chat.completions.create.return_value = _make_llm_response("B")
        assert route_question("GMV 是什么意思？") == "rag"

    @patch("ai_layer.rag_engine.llm")
    def test_a_response_returns_nl2sql(self, mock_llm):
        mock_llm.chat.completions.create.return_value = _make_llm_response("A")
        assert route_question("今日订单数量是多少？") == "nl2sql"

    @patch("ai_layer.rag_engine.llm")
    def test_unexpected_response_defaults_to_nl2sql(self, mock_llm):
        mock_llm.chat.completions.create.return_value = _make_llm_response("C")
        assert route_question("random question") == "nl2sql"


# ══════════════════════════════════════════════════════════════════════════════
# rag_engine — rag_query
# ══════════════════════════════════════════════════════════════════════════════

@pytest.mark.skipif(not _rag_available, reason="ai_layer.rag_engine not importable")
class TestRagQuery:
    @patch("ai_layer.rag_engine.retrieve")
    @patch("ai_layer.rag_engine._batch_score_relevance")
    @patch("ai_layer.rag_engine._generate_answer")
    @patch("ai_layer.rag_engine._score_groundedness")
    def test_success_path_has_required_keys(
        self, mock_ground, mock_answer, mock_scores, mock_retrieve
    ):
        mock_retrieve.return_value = [
            {"text": "chunk1", "source": "doc.md", "distance": 0.1}
        ]
        mock_scores.return_value = [0.9]
        mock_answer.return_value = "答案内容"
        mock_ground.return_value = 0.85

        result = rag_query("什么是 GMV？")
        assert "answer" in result
        assert "sources" in result
        assert "retrieval_scores" in result
        assert "answer_confidence" in result
        assert "query_rewritten" in result

    @patch("ai_layer.rag_engine.retrieve")
    @patch("ai_layer.rag_engine._batch_score_relevance")
    @patch("ai_layer.rag_engine._rewrite_query")
    @patch("ai_layer.rag_engine._generate_answer")
    @patch("ai_layer.rag_engine._score_groundedness")
    def test_low_relevance_triggers_query_rewrite(
        self, mock_ground, mock_answer, mock_rewrite, mock_scores, mock_retrieve
    ):
        mock_retrieve.return_value = [
            {"text": "chunk1", "source": "doc.md", "distance": 0.9}
        ]
        # First relevance call → low score triggers rewrite, second → high score
        mock_scores.side_effect = [[0.2], [0.8]]
        mock_rewrite.return_value = "改写后的问题"
        mock_answer.return_value = "答案"
        mock_ground.return_value = 0.9

        result = rag_query("模糊问题")
        assert result["query_rewritten"] is True
        mock_rewrite.assert_called_once()


# ══════════════════════════════════════════════════════════════════════════════
# agents — AgentState
# ══════════════════════════════════════════════════════════════════════════════

@pytest.mark.skipif(not _agents_available, reason="ai_layer.agents not importable")
class TestAgentState:
    def test_has_all_required_keys(self):
        annotations = AgentState.__annotations__
        for key in ("messages", "next_agent", "goal", "agent_outputs",
                    "iterations", "final_answer"):
            assert key in annotations, f"Missing key: {key}"

    def test_can_be_constructed_as_plain_dict(self):
        state: AgentState = {
            "messages": [],
            "next_agent": "DataAgent",
            "goal": "test goal",
            "agent_outputs": [],
            "iterations": 0,
            "final_answer": "",
        }
        assert state["goal"] == "test goal"
        assert state["iterations"] == 0


# ══════════════════════════════════════════════════════════════════════════════
# agents — supervisor_node
# ══════════════════════════════════════════════════════════════════════════════

@pytest.mark.skipif(not _agents_available, reason="ai_layer.agents not importable")
class TestSupervisorNode:
    def _state(self, iterations: int = 0, agent_outputs=None):
        return {
            "messages": [],
            "next_agent": "",
            "goal": "分析今日 GMV",
            "agent_outputs": agent_outputs or [],
            "iterations": iterations,
            "final_answer": "",
        }

    @patch("ai_layer.agents._get_llm")
    def test_valid_json_sets_next_agent(self, mock_get_llm):
        mock_llm = MagicMock()
        mock_get_llm.return_value = mock_llm
        mock_llm.invoke.return_value = MagicMock(
            content='{"next": "DataAgent", "reason": "需要查询数据"}'
        )
        result = supervisor_node(self._state(iterations=0))
        assert result["next_agent"] == "DataAgent"
        assert result["iterations"] == 1

    @patch("ai_layer.agents._get_llm")
    def test_iterations_gte_8_forces_finish_without_llm(self, mock_get_llm):
        result = supervisor_node(self._state(iterations=8))
        assert result["next_agent"] == "FINISH"
        mock_get_llm.assert_not_called()

    @patch("ai_layer.agents._get_llm")
    def test_invalid_json_falls_back_to_finish(self, mock_get_llm):
        mock_llm = MagicMock()
        mock_get_llm.return_value = mock_llm
        mock_llm.invoke.return_value = MagicMock(content="not valid json")
        result = supervisor_node(self._state(iterations=0))
        assert result["next_agent"] == "FINISH"

    @patch("ai_layer.agents._get_llm")
    def test_unknown_agent_name_falls_back_to_finish(self, mock_get_llm):
        mock_llm = MagicMock()
        mock_get_llm.return_value = mock_llm
        mock_llm.invoke.return_value = MagicMock(
            content='{"next": "GhostAgent", "reason": "..."}'
        )
        result = supervisor_node(self._state(iterations=0))
        assert result["next_agent"] == "FINISH"


# ══════════════════════════════════════════════════════════════════════════════
# agents — route_supervisor
# ══════════════════════════════════════════════════════════════════════════════

@pytest.mark.skipif(not _agents_available, reason="ai_layer.agents not importable")
class TestRouteSupervisor:
    def test_returns_next_agent_from_state(self):
        state: AgentState = {
            "messages": [],
            "next_agent": "AnomalyAgent",
            "goal": "test",
            "agent_outputs": [],
            "iterations": 1,
            "final_answer": "",
        }
        assert route_supervisor(state) == "AnomalyAgent"

    def test_returns_finish_when_state_says_finish(self):
        state: AgentState = {
            "messages": [],
            "next_agent": "FINISH",
            "goal": "done",
            "agent_outputs": [],
            "iterations": 9,
            "final_answer": "",
        }
        assert route_supervisor(state) == "FINISH"


# ══════════════════════════════════════════════════════════════════════════════
# agents — synthesize_node
# ══════════════════════════════════════════════════════════════════════════════

@pytest.mark.skipif(not _agents_available, reason="ai_layer.agents not importable")
class TestSynthesizeNode:
    def _state(self, agent_outputs=None):
        return {
            "messages": [],
            "next_agent": "FINISH",
            "goal": "分析 GMV",
            "agent_outputs": agent_outputs or [],
            "iterations": 3,
            "final_answer": "",
        }

    @patch("ai_layer.agents._get_llm")
    def test_sets_final_answer_key(self, mock_get_llm):
        mock_llm = MagicMock()
        mock_get_llm.return_value = mock_llm
        mock_llm.invoke.return_value = MagicMock(content="综合分析报告内容")
        result = synthesize_node(self._state(
            agent_outputs=[{"agent": "DataAgent", "output": "GMV=100万"}]
        ))
        assert "final_answer" in result
        assert result["final_answer"] == "综合分析报告内容"

    @patch("ai_layer.agents._get_llm")
    def test_goal_and_outputs_included_in_prompt(self, mock_get_llm):
        mock_llm = MagicMock()
        mock_get_llm.return_value = mock_llm
        mock_llm.invoke.return_value = MagicMock(content="报告")
        state = self._state(
            agent_outputs=[{"agent": "InsightAgent", "output": "洞察 XYZ"}]
        )
        state["goal"] = "test_goal_unique"
        synthesize_node(state)
        messages_arg = mock_llm.invoke.call_args[0][0]
        prompt_text = messages_arg[0].content
        assert "test_goal_unique" in prompt_text
        assert "InsightAgent" in prompt_text


# ══════════════════════════════════════════════════════════════════════════════
# agents — _run
# ══════════════════════════════════════════════════════════════════════════════

@pytest.mark.skipif(not _agents_available, reason="ai_layer.agents not importable")
class TestRun:
    @patch("ai_layer.agents._get_graph")
    def test_returns_output_and_intermediate_steps(self, mock_get_graph):
        mock_graph = MagicMock()
        mock_get_graph.return_value = mock_graph
        mock_graph.invoke.return_value = {
            "final_answer": "最终报告",
            "agent_outputs": [
                {"agent": "DataAgent", "output": "数据查询结果"},
            ],
        }
        result = _run("分析 GMV")
        assert result["output"] == "最终报告"
        assert len(result["intermediate_steps"]) == 1
        assert result["intermediate_steps"][0]["agent"] == "DataAgent"

    @patch("ai_layer.agents._get_graph")
    def test_empty_agent_outputs_yields_empty_steps(self, mock_get_graph):
        mock_graph = MagicMock()
        mock_get_graph.return_value = mock_graph
        mock_graph.invoke.return_value = {
            "final_answer": "done",
            "agent_outputs": [],
        }
        result = _run("short goal")
        assert result["intermediate_steps"] == []

    @patch("ai_layer.agents._get_graph")
    def test_intermediate_steps_contain_agent_and_output_keys(self, mock_get_graph):
        mock_graph = MagicMock()
        mock_get_graph.return_value = mock_graph
        mock_graph.invoke.return_value = {
            "final_answer": "结论",
            "agent_outputs": [
                {"agent": "AnomalyAgent", "output": "发现3个异常"},
                {"agent": "InsightAgent", "output": "洞察结论"},
            ],
        }
        result = _run("异常分析")
        for step in result["intermediate_steps"]:
            assert "agent" in step
            assert "output" in step


# ══════════════════════════════════════════════════════════════════════════════
# agents — run_free_agent
# ══════════════════════════════════════════════════════════════════════════════

@pytest.mark.skipif(not _agents_available, reason="ai_layer.agents not importable")
class TestRunFreeAgent:
    @patch("ai_layer.agents._run")
    def test_calls_run_with_user_goal(self, mock_run):
        mock_run.return_value = {"output": "结果", "intermediate_steps": []}
        result = run_free_agent("分析 Kappa 架构状态")
        mock_run.assert_called_once_with("分析 Kappa 架构状态")
        assert result["output"] == "结果"

    @patch("ai_layer.agents._run")
    def test_passes_goal_verbatim(self, mock_run):
        mock_run.return_value = {"output": "", "intermediate_steps": []}
        goal = "唯一的目标字符串_12345"
        run_free_agent(goal)
        mock_run.assert_called_once_with(goal)
