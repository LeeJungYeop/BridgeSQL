"""
시각화 엔진
쿼리 결과를 분석하여 최적의 차트를 자동 추천하고 렌더링합니다.
"""

from dataclasses import dataclass
from enum import Enum
from typing import Any
import json

import pandas as pd


class ChartType(str, Enum):
    """차트 유형"""
    BAR = "bar"
    LINE = "line"
    PIE = "pie"
    HORIZONTAL_BAR = "horizontal_bar"
    SCATTER = "scatter"
    TABLE = "table"  # 차트 추천 불가 시


@dataclass
class ChartRecommendation:
    """차트 추천 결과"""
    chart_type: ChartType
    x_column: str | None
    y_column: str | None
    title: str
    reason: str
    confidence: float


class ChartRecommender:
    """쿼리 결과 기반 차트 자동 추천"""
    
    def recommend(self, df: pd.DataFrame, question: str = "") -> ChartRecommendation:
        """
        데이터프레임을 분석하여 최적의 차트 추천
        
        Args:
            df: 쿼리 결과 데이터프레임
            question: 원본 자연어 질문 (힌트용)
        """
        if df.empty:
            return ChartRecommendation(
                chart_type=ChartType.TABLE,
                x_column=None,
                y_column=None,
                title="결과 없음",
                reason="데이터가 없습니다.",
                confidence=0.0,
            )
        
        columns = df.columns.tolist()
        n_rows = len(df)
        n_cols = len(columns)
        
        # 컬럼 타입 분석
        categorical_cols = df.select_dtypes(include=["object", "category"]).columns.tolist()
        numeric_cols = df.select_dtypes(include=["number"]).columns.tolist()
        datetime_cols = df.select_dtypes(include=["datetime"]).columns.tolist()
        
        # 시간 데이터 감지 (컬럼명 기반)
        time_keywords = ["date", "time", "날짜", "일자", "월", "년", "week", "month", "year"]
        potential_time_cols = [c for c in columns if any(k in c.lower() for k in time_keywords)]
        
        # 질문에서 힌트 추출
        question_lower = question.lower()
        wants_trend = any(k in question_lower for k in ["추이", "변화", "trend", "시간", "월별", "일별"])
        wants_ranking = any(k in question_lower for k in ["top", "순위", "가장", "best", "worst", "높은", "낮은"])
        wants_distribution = any(k in question_lower for k in ["분포", "비율", "구성", "비중", "percent"])
        
        # 추천 로직
        if n_rows == 1 and n_cols <= 5:
            # 단일 행 → 테이블
            return ChartRecommendation(
                chart_type=ChartType.TABLE,
                x_column=None,
                y_column=None,
                title="조회 결과",
                reason="단일 결과는 테이블로 표시",
                confidence=0.9,
            )
        
        if wants_trend and (datetime_cols or potential_time_cols) and numeric_cols:
            # 시간대별 추이 → Line Chart
            x_col = datetime_cols[0] if datetime_cols else potential_time_cols[0]
            y_col = numeric_cols[0]
            return ChartRecommendation(
                chart_type=ChartType.LINE,
                x_column=x_col,
                y_column=y_col,
                title=f"{y_col} 추이",
                reason="시간에 따른 변화는 Line Chart가 적합",
                confidence=0.85,
            )
        
        if wants_ranking and n_rows <= 20 and categorical_cols and numeric_cols:
            # 순위 → Horizontal Bar
            x_col = categorical_cols[0]
            y_col = numeric_cols[0]
            return ChartRecommendation(
                chart_type=ChartType.HORIZONTAL_BAR,
                x_column=y_col,  # 가로 막대에서는 숫자가 x
                y_column=x_col,  # 카테고리가 y
                title=f"{y_col} 순위",
                reason="순위 비교는 Horizontal Bar가 적합",
                confidence=0.85,
            )
        
        if wants_distribution and n_rows <= 10 and categorical_cols and numeric_cols:
            # 비율/분포 → Pie Chart
            return ChartRecommendation(
                chart_type=ChartType.PIE,
                x_column=categorical_cols[0],
                y_column=numeric_cols[0],
                title=f"{numeric_cols[0]} 분포",
                reason="비율 분포는 Pie Chart가 적합",
                confidence=0.8,
            )
        
        if categorical_cols and numeric_cols:
            # 기본: 카테고리별 수치 → Bar Chart
            if n_rows <= 15:
                return ChartRecommendation(
                    chart_type=ChartType.BAR,
                    x_column=categorical_cols[0],
                    y_column=numeric_cols[0],
                    title=f"{categorical_cols[0]}별 {numeric_cols[0]}",
                    reason="카테고리별 수치 비교는 Bar Chart가 적합",
                    confidence=0.75,
                )
        
        if len(numeric_cols) >= 2:
            # 두 수치 간 관계 → Scatter
            return ChartRecommendation(
                chart_type=ChartType.SCATTER,
                x_column=numeric_cols[0],
                y_column=numeric_cols[1],
                title=f"{numeric_cols[0]} vs {numeric_cols[1]}",
                reason="두 수치의 관계는 Scatter Plot이 적합",
                confidence=0.7,
            )
        
        # 기본값: 테이블
        return ChartRecommendation(
            chart_type=ChartType.TABLE,
            x_column=None,
            y_column=None,
            title="조회 결과",
            reason="적합한 차트를 찾지 못해 테이블로 표시",
            confidence=0.5,
        )


class ChartRenderer:
    """Plotly 기반 차트 렌더링"""
    
    @staticmethod
    def render_to_html(df: pd.DataFrame, recommendation: ChartRecommendation) -> str:
        """차트를 HTML로 렌더링"""
        try:
            import plotly.express as px
            import plotly.io as pio
        except ImportError:
            return "<p>Plotly가 설치되지 않았습니다. pip install plotly</p>"
        
        chart_type = recommendation.chart_type
        x_col = recommendation.x_column
        y_col = recommendation.y_column
        title = recommendation.title
        
        fig = None
        
        if chart_type == ChartType.BAR:
            fig = px.bar(df, x=x_col, y=y_col, title=title)
        elif chart_type == ChartType.LINE:
            fig = px.line(df, x=x_col, y=y_col, title=title, markers=True)
        elif chart_type == ChartType.PIE:
            fig = px.pie(df, names=x_col, values=y_col, title=title)
        elif chart_type == ChartType.HORIZONTAL_BAR:
            fig = px.bar(df, x=x_col, y=y_col, title=title, orientation="h")
        elif chart_type == ChartType.SCATTER:
            fig = px.scatter(df, x=x_col, y=y_col, title=title)
        elif chart_type == ChartType.TABLE:
            return df.to_html(index=False, classes="table table-striped")
        
        if fig:
            fig.update_layout(
                template="plotly_white",
                font=dict(family="Noto Sans KR, sans-serif"),
            )
            return pio.to_html(fig, include_plotlyjs="cdn", full_html=False)
        
        return df.to_html(index=False)
    
    @staticmethod
    def render_to_json(df: pd.DataFrame, recommendation: ChartRecommendation) -> dict:
        """차트 데이터를 JSON으로 반환 (프론트엔드용)"""
        return {
            "chart_type": recommendation.chart_type.value,
            "x_column": recommendation.x_column,
            "y_column": recommendation.y_column,
            "title": recommendation.title,
            "reason": recommendation.reason,
            "data": df.to_dict(orient="records"),
        }
