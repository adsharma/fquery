import timeit
from dataclasses import dataclass
from datetime import datetime

from fquery.sqlmodel import model


@model()
@dataclass
class User:
    id: int
    name: str
    email: str
    created_at: datetime = None
    updated_at: datetime = None


def create_user():
    return User(1, "John Doe", "john@example.com")


def create_user_sqlmodel():
    return User(1, "John Doe", "john@example.com").sqlmodel()


# Run the benchmark
num_iterations = 100000
time_taken = timeit.timeit(create_user, number=num_iterations)

print(f"Creating {num_iterations} User objects took {time_taken:.6f} seconds")
print(
    f"Average time per object creation: {(time_taken / num_iterations) * 1e9:.2f} nanoseconds"
)

# Run the benchmark
num_iterations = 100000
time_taken = timeit.timeit(create_user_sqlmodel, number=num_iterations)

print(f"Creating {num_iterations} User SQL Model objects took {time_taken:.6f} seconds")
print(
    f"Average time per object creation: {(time_taken / num_iterations) * 1e9:.2f} nanoseconds"
)
