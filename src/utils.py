import numpy as np
import pandas as pd
import uuid
import random



class BaseEntity:
    def __init__(self, **kwargs):
        for key, value in kwargs.items():
            setattr(self, key, value)

    def __repr__(self):
        attrs = ', '.join(f"{key}={value}" for key, value in self.__dict__.items())
        return f"{self.__class__.__name__}({attrs})"
    
    def showDataFrame(self):
        return pd.DataFrame([self.__dict__])


class Store(BaseEntity):
    all_stores = []  # Class-level list to store all instances of Store

    def __init__(self, store_id, latitude, longitude, store_name, opendt):
        super().__init__(STORE_ID=store_id, 
                         LATITUDE=latitude, 
                         LONGITUDE=longitude, 
                         STORE_NAME=store_name, 
                         OPENDT=opendt)
        Store.all_stores.append(self)  # Add each instance to the class-level list

    def __repr__(self):
        return f"Store(store_id={self.STORE_ID}, lat={self.LATITUDE}, lon={self.LONGITUDE}, name={self.STORE_NAME}, opendt={self.OPENDT})"

    @classmethod
    def display_all_stores(cls):
        """
        Display a DataFrame of all store instances.
        """
        return pd.DataFrame([store.__dict__ for store in cls.all_stores])
    

class Customer(BaseEntity):
    all_customers = []  # Class-level list to store all instances of Customers
    def __init__(self, customer_id, latitude, longitude, firstname, lastname, homestore_ID):
        super().__init__(CUSTOMER_ID=customer_id, 
                         LATITUDE=latitude, 
                         LONGITUDE=longitude, 
                         FIRSTNAME=firstname, 
                         LASTNAME=lastname, 
                         HOMESTORE_ID=homestore_ID)
        Customer.all_customers.append(self)  # Add each instance to the class-level list

    def __repr__(self):
        return f"Customer(customer_id={self.CUSTOMER_ID}, lat={self.LATITUDE}, lon={self.LONGITUDE}, name={self.FIRSTNAME} {self.LASTNAME}, homestoreID={self.HOMESTORE_ID})"
    
    @classmethod
    def display_all_customers(cls):
        """
        Display a DataFrame of all customer instances.
        """
        return pd.DataFrame([cust.__dict__ for cust in cls.all_customers])


class Order(BaseEntity):
    all_orders = []  # Class-level list to store all instances of Orders
    def __init__(self, order_id, customer_id, store_id, order_date, subtotal,tax, total):        
        super().__init__(ORDER_ID=order_id, 
                         CUSTOMER_ID=customer_id, 
                         STORE_ID=store_id, 
                         ORDER_DATE=order_date, 
                         SUBTOTAL=subtotal,
                         TAX=tax,
                         TOTAL=total)

    @classmethod
    def display_all_orders(cls):
        """
        Display a DataFrame of all order instances.
        """
        return pd.DataFrame([order.__dict__ for order in cls.all_orders])
    

def generate_int_uuid(int_length):
    """
    Generate a random integer UUID of specified length.
    """
    if int_length < 1:
        raise ValueError("int_length must be at least 1")
    if int_length > 10:
        raise ValueError("int_length must be less than or equal to 10")
    
    return uuid.uuid4().int % (10 ** int_length)  # Modulo to limit the size

def generate_random_date(start_date, end_date):
    """
    Generate a random date between start_date and end_date.
    """
    delta = end_date - start_date
    random_days = random.randint(0, delta.days)
    return start_date + pd.Timedelta(days=random_days)

def generate_store_locations(num_stores, min_lat, max_lat, min_lon, max_lon, seed=None):
    """
    Generate random store locations within the specified bounding box.
    """
    random.seed(seed)  # For reproducibility

    Store.all_stores = []  # Reset the class-level list to avoid duplicates

    store_locations = []
    for _ in range(num_stores):
        # Generate random store ID 
        store_id = generate_int_uuid(5)  # Generate a random store ID (0-9999)

        # Generate random latitude and longitude within the bounding box
        latitude = np.round(random.uniform(min_lat, max_lat), 5)
        longitude = np.round(random.uniform(min_lon, max_lon), 5)

        store_name = f"Panucci's Pizza - {store_id}"
        opendt = f"{random.randint(2010, 2020)}-{random.randint(1, 12):02d}-{random.randint(1, 28):02d}"
        store_locations.append(Store(store_id=store_id,
                                     latitude=latitude,
                                     longitude=longitude,
                                     store_name=store_name,
                                     opendt=opendt))
    return store_locations



def generate_customers(num_customers_range, search_radius_range, first_names=None, last_names=None, seed=None):

    from scipy.stats import gamma
    random.seed(seed)  # For reproducibility

    # Predefined list of first names
    first_names = ["Alice", "Bob", "Charlie", "David", "Eve", "Frank", "Grace", "Hannah", "Ivy", "Jack", "Kathy", "Liam", "Mona", "Nathan", "Olivia", "Paul", "Quincy", "Rachel", "Steve", "Tina"]
    # Predefined list of Pokémon names as last names
    last_names = ["Pikachu", "Charmander", "Bulbasaur", "Squirtle", "Jigglypuff", "Meowth", "Psyduck", "Snorlax", "Eevee", "Mewtwo"]

    customers = []
    Customer.all_customers = []  # Reset the class-level list to avoid duplicates

    for store in Store.all_stores:
        num_customers = random.randint(*num_customers_range)  # Randomly choose the number of customers for each store
        search_radius = random.uniform(*search_radius_range)  # Randomly choose the search radius for each store
        store_lat = store.LATITUDE
        store_lon = store.LONGITUDE 
        for i in range(num_customers):
            # Generate a random distance using exponential distribution
            distance = np.random.exponential(scale=search_radius / 2)
            # Generate a random bearing (angle in radians)
            bearing = np.random.uniform(0, 2 * np.pi)
            # Calculate the new latitude and longitude
            delta_lat = distance * np.cos(bearing) / 111  # Approx. conversion of km to degrees latitude
            delta_lon = distance * np.sin(bearing) / (111 * np.cos(np.radians(store_lat)))  # Adjust for longitude
            customer_lat = store_lat + delta_lat
            customer_lon = store_lon + delta_lon
            # Create a Customer object
            customer_id = generate_int_uuid(5)  # Generate a random customer ID (0-9999)
            first_name = random.choice(first_names)
            last_name = random.choice(last_names)
            customers.append(Customer(customer_id, np.round(customer_lat,5), np.round(customer_lon,5), first_name, last_name, store.STORE_ID))

    return customers

def generate_orders(num_orders_per_customer = (3,25), seed=None):
    def generate_gaussian_value(mean, std, floor):
        value = np.random.normal(loc=mean, scale=std)
        return round(max(value, floor), 2)  # Round to 2 decimal places and ensure it's not below the floor value

    Order.all_orders = []  # Reset the class-level list to avoid duplicates

    for customer in Customer.all_customers:
        num_orders = random.randint(*num_orders_per_customer)  # Randomly choose the number of orders for each customer
        for _ in range(num_orders):
            order_id = generate_int_uuid(8)
            order_date = generate_random_date(pd.Timestamp('2021-01-01'), 
                                              pd.Timestamp('2024-12-31'))
            order_subtotal = generate_gaussian_value(mean=30, std=10, floor=5)  # Generate a random order total
            order_tax = np.round(order_subtotal * 0.0825,2)  # Assuming a tax rate of 8.25%
            order_total = np.round(order_subtotal + order_tax,2)
            Order.all_orders.append(Order(order_id, customer.CUSTOMER_ID, customer.HOMESTORE_ID, order_date, order_subtotal, order_tax, order_total))



# Test the classes and functions
if __name__ == "__main__":
    # Generate store locations
    stores = generate_store_locations(num_stores=5, min_lat=35.0, max_lat=36.0, min_lon=-120.0, max_lon=-119.0)
    print(Store.display_all_stores())

    # Generate customers for the first store
    store_location = (stores[0].LATITUDE, stores[0].LONGITUDE)
    customers = generate_customers(num_customers_range=(3,25), search_radius_range=(1.3,3.0))
    print(Customer.display_all_customers())

    # Generate orders for all customers
    orders = generate_orders()
    print(Order.display_all_orders())