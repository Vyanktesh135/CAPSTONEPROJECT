from pydantic import AnyUrl
from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    DATABASE_URL: AnyUrl
    PRODUCTION: bool
    OPENAI_API_KEY: str
    class Config:
        env_file = ".env"
        enf_file_encoding = "utf-8"
    
settings = Settings()