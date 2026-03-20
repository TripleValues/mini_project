from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
  mariadb_host: str
  jdbc_url: str
  db_user: str
  db_password: str
  db_driver: str
  spark_url: str
  host_ip: str
  file_dir: str
  hadoop_path: str

  model_config = SettingsConfigDict(
    env_file=".env",
    env_file_encoding="utf-8",
  )

settings = Settings()
