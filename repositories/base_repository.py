from sqlalchemy.orm import Session

class BaseRepository:
    def __init__(self, session: Session, model):
        self.session = session
        self.model = model

    def get_by_id(self, id: int):
        return self.session.query(self.model).filter(self.model.id == id).first()

    def get_all(self, **filters):
        query = self.session.query(self.model)
        if filters:
            query = query.filter_by(**filters)
        return query.all()

    async def create(self, **data):
        instance = self.model(**data)
        self.session.add(instance)
        await self.await session.commit()
        self.session.refresh(instance)
        return instance

    async def update(self, id: int, **data):
        instance = self.get_by_id(id)
        if instance:
            for key, value in data.items():
                setattr(instance, key, value)
            await self.await session.commit()
            self.session.refresh(instance)
        return instance

    async def delete(self, id: int):
        instance = self.get_by_id(id)
        if instance:
            await self.await session.delete(instance)
            await self.await session.commit()
            return True
        return False
