import ray

# start ray
ray.init()

@ray.remote(max_restarts=5, max_task_retries=-1)
class Account:
    def __init__(self, balance: float, minimal_balance: float):
        # initialize account and balance
        self.minimal = minimal_balance
        if balance < minimal_balance:
            raise Exception("ERROR :: Starting balance is less than minimal balance")
        self.balance = balance

    def balance(self) -> float:
        # get current balance (private state)
        return self.balance

    def deposit(self, amount: float) -> float:
        # take deposit and update balance state
        if amount < 0:
            raise Exception("ERROR :: Cannot deposit negative amount")
        self.balance = self.balance + amount
        return self.balance

    def withdraw(self, amount: float) -> float:
        # release withdraw and update balance state
        if amount < 0:
            raise Exception("ERROR :: Cannot withdraw negative amount")
        balance = self.balance - amount
        if balance < self.minimal:
            raise Exception("ERROR :: Withdrawal is not supported by current balance")
        self.balance = balance
        return balance

# invoke remote actor instance
account_actor =  Account.remote(balance = 99.,minimal_balance=11.)

# do procedure calls to interact with instance
print(f"INFO :: Current Balance: {ray.get(account_actor.balance.remote())}")
print(f"INFO :: Balance after Withdraw: {ray.get(account_actor.withdraw.remote(66.))}")
print(f"INFO :: Balance after Deposit: {ray.get(account_actor.deposit.remote(33.))}")