import os


class Settings:
    PROJECT_NAME: str = os.getenv("PROJECT_NAME", "pulse-backend")
    DEBUG: bool = os.getenv("DEBUG", "true").lower() in ("1", "true", "yes")


settings = Settings()
