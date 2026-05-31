from pydantic import BaseModel

class MongoModel(BaseModel):
    def model_dump(self, **kwargs):
        return super().model_dump(
            mode="python", 
            by_alias=True, 
            exclude_none=True,
            **kwargs
        )