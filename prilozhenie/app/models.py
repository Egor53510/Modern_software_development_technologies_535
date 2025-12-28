from pydantic import BaseModel
from datetime import datetime, date
from typing import Optional, List, Dict, Any, Union
from enum import Enum


class AuthorBase(BaseModel):
    first_name: str
    last_name: str
    biography: Optional[str] = None
    birth_date: Optional[date] = None
    is_active: bool = True

class AuthorCreate(AuthorBase):
    pass

class Author(AuthorBase):
    author_id: int
    created_at: datetime

    class Config:
        orm_mode = True
#-------------------------
class PublisherBase(BaseModel):
    name: str
    address: Optional[str] = None
    phone: Optional[str] = None
    email: Optional[str] = None

class PublisherCreate(PublisherBase):
    pass


class Publisher(PublisherBase):
    publisher_id: int
    created_at: datetime

    class Config:
        orm_mode = True
#-------------------------
class GenreBase(BaseModel):
    name: str
    description: Optional[str] = None

class GenreCreate(GenreBase):
    pass

class Genre(GenreBase):
    genre_id: int
    created_at: datetime

    class Config:
        orm_mode = True

#-------------------------
class BookBase(BaseModel):
    isbn: Optional[str] = None
    title: str
    publisher_id: Optional[int] = None
    publisher_year: Optional[int] = None
    page_count: Optional[int] = None
    price: float
    quantity_in_stock: int = 1
    description: Optional[str] = None
    language: str = "ru"

class BookCreate(BookBase):
    pass

class Book(BookBase):
    book_id: int
    created_at: datetime
    author_id: int
    publisher_id: int
    genre_id: int

    class Config:
        orm_mode = True

#-------------------------
class ReaderBase(BaseModel):
    first_name: str
    last_name: str
    email: str
    phone: Optional[str] = None
    address: Optional[str] = None
    is_active: bool = True
    notes: Optional[str] = None

class ReaderCreate(ReaderBase):
    pass

class Reader(ReaderBase):
    reader_id: int
    registration_date: datetime
    
    class Config:
        orm_mode = True

#-------------------------
class BookLoanBase(BaseModel):
    book_id: int
    reader_id: int
    due_date: datetime
    return_date: Optional[datetime] = None
    fine_amount: float = 0.0
    is_returned: bool = False
    notes: Optional[str] = None

class BookLoanCreate(BookLoanBase):
    pass

class BookLoan(BookLoanBase):
    loan_id: int
    loan_date: datetime
    
    class Config:
        orm_mode = True

#-------------------------
class FineBase(BaseModel):
    loan_id: int
    amount: float
    reason: str
    is_paid: bool = False
    paid_date: Optional[datetime] = None

class FineCreate(FineBase):
    pass

class Fine(FineBase):
    fine_id: int
    issue_date: datetime
    
    class Config:
        orm_mode = True

#-------------------------
class SQLQuery(BaseModel):
    query: str
    params: Optional[Dict[str, Any]] = None

class ExportFormat(str, Enum):
    EXCEL = "excel"
    JSON = "json"
    CSV = "csv"

class BackupRequest(BaseModel):
    backup_name: Optional[str] = None
    tables: Optional[List[str]] = None

class ArchiveRequest(BaseModel):
    tables: List[str]
    reason: str