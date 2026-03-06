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

    def create(self, **data):
        instance = self.model(**data)
        self.session.add(instance)
        self.session.commit()
        self.session.refresh(instance)
        return instance

    def update(self, id: int, **data):
        instance = self.get_by_id(id)
        if instance:
            for key, value in data.items():
                setattr(instance, key, value)
            self.session.commit()
            self.session.refresh(instance)
        return instance

    def delete(self, id: int):
        instance = self.get_by_id(id)
        if instance:
            self.session.delete(instance)
            self.session.commit()
            return True
        return False
