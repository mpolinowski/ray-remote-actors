import ray
from os.path import exists

ray.init()

@ray.remote
class Account:
    def __init__(self, balance: float, minimal_balance: float, account_key: str, basedir: str = '.'):
        # locate the file storage location
        self.basedir = basedir
        # set an account identification
        self.key = account_key
        # if state was not restored create new from props
        if not self.restorestate():
            if balance < minimal_balance:
                raise Exception("ERROR :: Starting balance is less then minimal balance")
            self.balance = balance
            self.minimal = minimal_balance
            # write generated state to file
            self.storestate()

    def balance(self) -> float:
        # get current balance (private state)
        return self.balance

    def deposit(self, amount: float) -> float:
        # take deposit and update balance state
        if amount < 0:
            raise Exception("ERROR :: Cannot deposit negative amount")
        self.balance = self.balance + amount
        self.storestate()
        return self.balance

    def withdraw(self, amount: float) -> float:
        # release withdraw and update balance state
        if amount < 0:
            raise Exception("ERROR :: Can not withdraw negative amount")
        balance = self.balance - amount
        if balance < self.minimal:
            raise Exception("ERROR :: Withdraw is not supported by current balance")
        self.balance = balance
        self.storestate()
        return balance

    def restorestate(self) -> bool:
        # if stored state for account id exist load it
        if exists(self.basedir + '/' + self.key):
            with open(self.basedir + '/' + self.key, "rb") as f:
                bytes = f.read()
            state = ray.cloudpickle.loads(bytes)
            self.balance = state['balance']
            self.minimal = state['minimal']
            return True
        else:
            return False

    def storestate(self):
        # store state to file
        bytes = ray.cloudpickle.dumps({'balance' : self.balance, 'minimal' : self.minimal})
        with open(self.basedir + '/' + self.key, "wb") as f:
            f.write(bytes) 

# invoke an instance of the account worker
account_actor = Account.options(name='Account')\
    .remote(balance=99.,minimal_balance=11., account_key='secretaccountkey')

# make changes to it's default state
print(f"INFO :: Current balance: {ray.get(account_actor.balance.remote())}")
print(f"INFO :: Balance after Withdraw: {ray.get(account_actor.withdraw.remote(66.))}")
print(f"INFO :: Balance after Deposit: {ray.get(account_actor.deposit.remote(33.))}")

# get actor id
print(ray.get_actor('Account'))

# kill the first instance
ray.kill(account_actor)

# and create a new one
account_actor = Account.options(name='Account') \
    .remote(balance=99.,minimal_balance=11., account_key='secretaccountkey')

# it should have restored the state from before
print(f"INFO :: Current balance {ray.get(account_actor.balance.remote())}")

# verify that this is a new actor
print(ray.get_actor('Account'))

# kill the first instance
ray.kill(account_actor)
