FROM python:3.13.11 as build

RUN apt-get update
RUN apt-get upgrade -y
RUN curl -LsSf https://astral.sh/uv/install.sh | sh
RUN pip install uv

WORKDIR /workspace

COPY ./app1/pyproject.toml ./app1/uv.lock ./

RUN uv sync --frozen --no-dev

COPY ./app1/.env ./.env
COPY ./app1/src ./src

FROM python:3.13.11

WORKDIR /workspace

COPY --from=build /workspace/.venv /workspace/.venv
COPY --from=build /workspace/.env /workspace/.env
COPY --from=build /workspace/src /workspace/src

ENV PATH="/workspace/.venv/bin:$PATH"

EXPOSE 8000

CMD ["uvicorn", "src.main:app", "--host", "0.0.0.0", "--port", "8000"]
