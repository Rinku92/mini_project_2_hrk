from operator import or_, and_, not_
from pprint import pprint
from typing import cast
from sqlalchemy import cast, Date, distinct, union
from sqlalchemy import create_engine, MetaData, Table, Integer, String, \
    Column, DateTime, ForeignKey, Numeric, SmallInteger, desc, func, distinct, Date
from sqlalchemy import text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, Session, sessionmaker
from sqlalchemy import update
from sqlalchemy.exc import IntegrityError
from datetime import datetime

engine = create_engine("sqlite:////web/Sqlite-Data/example.db")
Base = declarative_base()
session = Session(bind=engine)

class Customer(Base):
    __tablename__ = 'customers'
    id = Column(Integer(), primary_key=True)
    first_name = Column(String(100), nullable=False)
    last_name = Column(String(100), nullable=False)
    username = Column(String(50), nullable=False)
    email = Column(String(200), nullable=False)
    address = Column(String(200), nullable=False)
    town = Column(String(200), nullable=False)
    created_on = Column(DateTime(), default=datetime.now)
    updated_on = Column(DateTime(), default=datetime.now, onupdate=datetime.now)
    orders = relationship("Order", backref='customer')


class Item(Base):
    __tablename__ = 'items'
    id = Column(Integer(), primary_key=True)
    name = Column(String(200), nullable=False)
    cost_price = Column(Numeric(10, 2), nullable=False)
    selling_price = Column(Numeric(10, 2), nullable=False)
    quantity = Column(Integer(), nullable=False)
    orders = relationship("OrderLine", backref='item')


class Order(Base):
    __tablename__ = 'orders'
    id = Column(Integer(), primary_key=True)
    customer_id = Column(Integer(), ForeignKey('customers.id'))
    date_placed = Column(DateTime(), default=datetime.now)
    line_items = relationship("OrderLine", backref='order')


class OrderLine(Base):
    __tablename__ = 'order_lines'
    id = Column(Integer(), primary_key=True)
    order_id = Column(Integer(), ForeignKey('orders.id'))
    item_id = Column(Integer(), ForeignKey('items.id'))
    quantity = Column(SmallInteger())



Base.metadata.create_all(engine)

Base.metadata.bind = engine

DBSession = sessionmaker(bind=engine)
session = DBSession()
# Insert customer in customer table
c1 = Customer(first_name='Toby',
              last_name='Miller',
              username='tmiller',
              email='tmiller@example.com',
              address = '1662 Kinney Street',
              town = 'Wolfden'
              )

c2 = Customer(first_name='Scott',
              last_name='Harvey',
              username='scottharvey',
              email='scottharvey@example.com',
              address = '424 Patterson Street',
              town = 'Beckinsdale'
              )

session.add(c1)
session.add(c2)
session.commit()

c3 = Customer(
    first_name="John",
    last_name="Lara",
    username="johnlara",
    email="johnlara@mail.com",
    address="3073 Derek Drive",
    town="Norfolk"
)

c4 = Customer(
    first_name="Sarah",
    last_name="Tomlin",
    username="sarahtomlin",
    email="sarahtomlin@mail.com",
    address="3572 Poplar Avenue",
    town="Norfolk"
)

c5 = Customer(first_name='Toby',
              last_name='Miller',
              username='tmiller',
              email='tmiller@example.com',
              address='1662 Kinney Street',
              town='Wolfden'
              )

c6 = Customer(first_name='Scott',
              last_name='Harvey',
              username='scottharvey',
              email='scottharvey@example.com',
              address='424 Patterson Street',
              town='Beckinsdale'
              )

session.add_all([c3, c4, c5, c6])
session.commit()

# Insert items in Items Table
i1 = Item(name='Chair', cost_price=9.21, selling_price=10.81, quantity=5)
i2 = Item(name='Pen', cost_price=3.45, selling_price=4.51, quantity=3)
i3 = Item(name='Headphone', cost_price=15.52, selling_price=16.81, quantity=50)
i4 = Item(name='Travel Bag', cost_price=20.1, selling_price=24.21, quantity=50)
i5 = Item(name='Keyboard', cost_price=20.1, selling_price=22.11, quantity=50)
i6 = Item(name='Monitor', cost_price=200.14, selling_price=212.89, quantity=50)
i7 = Item(name='Watch', cost_price=100.58, selling_price=104.41, quantity=50)
i8 = Item(name='Water Bottle', cost_price=20.89, selling_price=25, quantity=50)

session.add_all([i1, i2, i3, i4, i5, i6, i7, i8])
session.commit()

#Insert Oders in Orders Table

o1 = Order(customer = c1)
o2 = Order(customer = c1)

line_item1 = OrderLine(order = o1, item = i1, quantity =  3)
line_item2 = OrderLine(order = o1, item = i2, quantity =  2)
line_item3 = OrderLine(order = o2, item = i1, quantity =  1)
line_item3 = OrderLine(order = o2, item = i2, quantity =  4)

session.add_all([o1, o2])

session.new
session.commit()

o3 = Order(customer=c1)
orderline1 = OrderLine(item=i1, quantity=5)
orderline2 = OrderLine(item=i2, quantity=10)

#o3.order_lines.append(orderline1)
#o3.order_lines.append(orderline2)

session.add_all([o3])

session.commit()
# print orders attribute of the Customer object
#pprint(c1.orders)
# print customer attribute on the Order object
#pprint(o1.customer)

print("=========Customers/all()=========")
#prints all the records of  the Customers table
q = session.query(Customer).all()
for c in q:
   print ("customer: ",c.id," ",c.first_name)
print("===========================")

print("=========Items/all()=========")
#prints all the records of  the Items table
q = session.query(Item).all()
for c in q:
   print ("Items: ",c.id," ",c.name)
print("=========================")


print("=========Orders/all()=========")
#prints all the records of  the Orders table
q = session.query(Order).all()
for c in q:
   print ("Order: ",c.id)
print("=========================")

print("=========SQL Query for Customer=========")
#print SQL Query  for  customers
print(session.query(Customer))
print("===========================")

print("=========count()=========")
#print total number of records in each table
print(session.query(Customer).count())
print(session.query(Item).count())
print(session.query(Order).count())
print("===========================")

print("=========First()=========")
#The first() method returns the first result of the query or None if the query returns zero results.
q = session.query(Customer).first()
print ("customer: ",q.id," ",q.first_name)
q = session.query(Item).first()
print ("Items: ",q.id," ",q.name)
q = session.query(Order).first()
print("Order: ", q.id)
print("===========================")

print("=========Get()=========")
#The get() method returns the instance which matches the primary key passed to it or None if no such object found.
q = session.query(Customer).get(1)
print ("customer: ",q.id," ",q.first_name)
q = session.query(Item).get(1)
print ("Items: ",q.id," ",q.name)
print(session.query(Order).get(200))
print("===========================")

print("=========filter()=========")
q = session.query(Customer).filter(Customer.first_name == 'John').all()
# All customers with name starting with John
for c in q:
   print ("customer: ",c.id," ",c.first_name)
print("===========================")

q = session.query(Customer).filter(Customer.id <= 5, Customer.town.like("Nor%")).all()
#This query returns all the customers whose primary key is less than or equal to 5 and town name starts with Nor
for c in q:
   print ("customer: ",c.id," ",c.first_name)

print("=======find all customers who either live in Peterbrugh or Norfolk====================")
q=session.query(Customer).filter(or_(Customer.town == 'Peterbrugh', Customer.town == 'Norfolk')).all()
for c in q:
   print ("customer: ",c.id," ",c.first_name)

print("=======find all customers whose first name is John and live in Norfolk===================")
q=session.query(Customer).filter(and_(Customer.first_name == 'John', Customer.town == 'Norfolk')).all()
for c in q:
   print ("customer: ",c.id," ",c.first_name)

print("=======find all johns who don't live in Peterbrugh================")
q=session.query(Customer).filter(and_(Customer.first_name == 'John', not_(Customer.town == 'Peterbrugh', ))).all()
for c in q:
   print ("customer: ",c.id," ",c.first_name)
print("===========================")

print("=============IS NULL==============")
q = session.query(Order).filter(Order.date_placed == None).all()
for c in q:
      print("ID: ", c.id)

print("=============IS NOT NULL==============")
q = session.query(Order).filter(Order.date_placed != None).all()
for c in q:
      print("ID: ", c.id)

print("=============IN==============")
q = session.query(Customer).filter(Customer.first_name.in_(['Toby', 'Sarah'])).all()
for c in q:
   print ("customer: ",c.id," ",c.first_name)

print("============NOT IN==============")
q = session.query(Customer).filter(Customer.first_name.notin_(['Toby', 'Sarah'])).all()
for c in q:
   print ("customer: ",c.id," ",c.first_name)

print("============BETWEEN==============")
q= session.query(Item).filter(Item.cost_price.between(10, 50)).all()
for c in q:
   print ("Items: ",c.id," ",c.name)

print("============NOT BETWEEN==============")
q = session.query(Item).filter(Item.cost_price.between(10, 50)).all()
for c in q:
   print ("Items: ",c.id," ",c.name)

print("============ LIKE ==============")
q = session.query(Item).filter(Item.name.like("%r")).all()
for c in q:
   print ("Items: ",c.id," ",c.name)

print("============ NOT LIKE ==============")
q = session.query(Item).filter(Item.name.ilike("w%")).all()
for c in q:
   print ("Items: ",c.id," ",c.name)

print("===========limit() ==============")
#The limit() method adds LIMIT clause to the query. It accepts the number of rows you want to return from the query.
q= session.query(Customer).limit(2).all()
for c in q:
   print ("customer: ",c.id," ",c.first_name)
print("===========================")
q=session.query(Customer).filter(Customer.address.ilike("%avenue")).limit(2).all()
for c in q:
   print ("customer: ",c.id," ",c.first_name)

print("===========offset()==============")
#The offset() method adds the OFFSET clause to the query. It accepts offset as an argument. It is commonly used with the limit() clause.
q = session.query(Customer).limit(2).offset(2).all()
for c in q:
   print ("customer: ",c.id," ",c.first_name)
print("============SQL equivalent Query===============")
print(session.query(Customer).limit(2).offset(2))
print("===========================")

print("===========Order_by()==============")
#The order_by() method is used to order the result by adding ORDER BY clause to the query. It accepts column names on which the order should be based. By default, it sorts in ascending order.
q = session.query(Item).filter(Item.name.ilike("wa%")).all()
for c in q:
   print ("Items: ",c.id," ",c.name)
print("===========================")
q= session.query(Item).filter(Item.name.ilike("wa%")).order_by(Item.cost_price).all()
for c in q:
   print ("Items: ",c.id," ",c.name)
print("=============desc() ==============")
q=session.query(Item).filter(Item.name.ilike("wa%")).order_by(desc(Item.cost_price)).all()
for c in q:
   print ("Items: ",c.id," ",c.name)

print("===========JOIN()==============")
#The join() method is used to create SQL INNER JOIN. It accepts table name for which you want to create SQL JOIN.
q = session.query(Customer).join(Order).all()
for c in q:
   print ("customer: ",c.id," ",c.first_name)
print("===========================")

print("===========OUTER JOIN()==============")
q= session.query(Customer.first_name,Order.id,).outerjoin(Order).all()
for c in q:
   print ("customer: ",c.first_name," ",c.id)
print("===========================")


print("=========group_by()===========")
#To We group results using the group_by() method. It accepts one or more columns and groups the rows according to the values in the column.
print(session.query(func.count(Customer.id)).join(Order).filter(
    Customer.first_name == 'John',
    Customer.last_name == 'Green',
).group_by(Customer.id).scalar())

print("===========Having()==============")
#To filter out the results based on the values returned by aggregate functions we use having() method which adds the HAVING clause to the SELECT statement. Just like the where() clause, it accepts a condition.
print("=======find the number of customers lives in each town===========")
q =session.query(func.count("*").label('town_count'), Customer.town).group_by(Customer.town).having(func.count("*") > 2).all()
print(q)


print("========Dealing with Duplicates======")
#To deal with the duplicate rows in the result set we use the DISTINCT option. We can add DISTINCT option to the SELECT statement using the distinct() method.
q=  session.query(Customer.town).filter(Customer.id  < 10).all()
print(q)
print("===========================")
q= session.query(Customer.town).filter(Customer.id  < 10).distinct().all()
print(q)
print("===========================")
q=session.query(func.count(distinct(Customer.town)),func.count(Customer.town)).all()
print(q)

print("=======UNIONS=============")
#TO union queries we use the union() method of the Query object.
s1 = session.query(Item.id, Item.name).filter(Item.name.like("Wa%"))
s2 = session.query(Item.id, Item.name).filter(Item.name.like("%e%"))
print(s1.union(s2).all())

#By default, union() removes all the duplicate rows from the result set. If you want to keep the duplicates use union_all().
print(s1.union_all(s2).all())

print("====Updating Data======")
#To update an object simply set its attribute to a new value, add the object to the session and commit the changes.
i = session.query(Item).get(8)
i.selling_price = 25.91
session.add(i)
session.commit()

#To update multiple rows at once use update() method of the Query object. It returns the total number of records updated.
print("===update quantity of all quantity of items to 60 whose name starts with 'W'===")
session.query(Item).filter(
    Item.name.ilike("W%")
).update({"quantity": 60}, synchronize_session='fetch')
session.commit()

print("====Deleting Data===")
#To delete an object use the delete() method of the session object. It accepts an object and marks it to be deleted in the next commit.
q=session.query(Item).filter(
    Item.name.ilike("W%")
).delete(synchronize_session='fetch')
print(q)
session.commit()


print("===Raw Queries==")
# ORM also give you the flexibility to directly to use directly use SQL using the text() function
q=session.query(Customer).filter(text("first_name = 'John'")).all()
for c in q:
   print ("customer: ",c.id," ",c.first_name)
   print("===========================")
q=session.query(Customer).filter(text("town like 'Nor%'")).all()
for c in q:
   print ("customer: ",c.id," ",c.first_name)
   print("===========================")
q=session.query(Customer).filter(text("town like 'Nor%'")).order_by(text("first_name, id desc")).all()
for c in q:
   print ("customer: ",c.id," ",c.first_name)

print("========Transactions============")

def dispatch_order(order_id):
    # check whether order_id is valid or not
    order = session.query(Order).get(order_id)

    if not order:
        raise ValueError("Invalid order id: {}.".format(order_id))

    if order.date_shipped:
        print("Order already shipped.")
        return

    try:
        for i in order.order_lines:
            i.item.quantity = i.item.quantity - i.quantity

        order.date_shipped = datetime.now()
        session.commit()
        print("Transaction completed.")

    except IntegrityError as e:
        print(e)
        print("Rolling back ...")
        session.rollback()
        print("Transaction failed.")
        dispatch_order(1)
