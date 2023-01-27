import ray
from os.path import exists
from ray.util import ActorPool

ray.init()

class BasePersitence:
    def exists(self, key:str) -> bool:
        pass
    def save(self, key: str, data: dict):
        pass
    def restore(self, key:str) -> dict:
        pass
        

# export persistence logic into separate class
@ray.remote
class FilePersistence(BasePersitence):
    def __init__(self, basedir: str = '.'):
        self.basedir = basedir

    def exists(self, key:str) -> bool:
        return exists(self.basedir + '/' + key)

    def save(self, keyvalue: ()):
        bytes = ray.cloudpickle.dumps(keyvalue[1])
        with open(self.basedir + '/' + keyvalue[0], "wb") as f:
            f.write(bytes)

    def restore(self, key:str) -> dict:
        if self.exists(key):
            with open(self.basedir + '/' + key, "rb") as f:
                bytes = f.read()
            return ray.cloudpickle.loads(bytes)
        else:
            return None
            

pool = ActorPool([FilePersistence.remote(), FilePersistence.remote(), FilePersistence.remote()])

@ray.remote
class Account:
    def __init__(self, balance: float, minimal_balance: float, account_key: str, persistence: ActorPool):
        self.persistence = persistence
        self.key = account_key
        if not self.restorestate():
            if balance < minimal_balance:
                raise Exception("ERROR :: Starting balance is less then minimal balance")
            self.balance = balance
            self.minimal = minimal_balance
            self.storestate()

    def balance(self) -> float:
        return self.balance

    def deposit(self, amount: float) -> float:
        if amount < 0:
            raise Exception("ERROR :: Can not deposit negative amount")
        self.balance = self.balance + amount
        self.storestate()
        return self.balance

    def withdraw(self, amount: float) -> float:
        if amount < 0:
            raise Exception("ERROR :: Can not withdraw negative amount")
        balance = self.balance - amount
        if balance < self.minimal:
            raise Exception("ERROR :: Withdraw is not supported by current balance")
        self.balance = balance
        self.storestate()
        return balance

    def restorestate(self) -> bool:
        while(self.persistence.has_next()):
            self.persistence.get_next()
        self.persistence.submit(lambda a, v: a.restore.remote(v), self.key)
        state = self.persistence.get_next()
        if state != None:
            print(f'INFO :: Restoring state {state}')
            self.balance = state['balance']
            self.minimal = state['minimal']
            return True
        else:
            return False

    def storestate(self):
        self.persistence.submit(lambda a, v: a.save.remote(v), (self.key,
                                    {'balance' : self.balance, 'minimal' : self.minimal}))

# invoke an instance of the account worker
account_actor = Account.options(name='Account').remote(balance=99.,minimal_balance=11.,
                                    account_key='secretaccountkey', persistence=pool)

# make changes to it's default state
print(f"INFO :: Current balance: {ray.get(account_actor.balance.remote())}")
print(f"INFO :: Balance after Withdraw: {ray.get(account_actor.withdraw.remote(66.))}")
print(f"INFO :: Balance after Deposit: {ray.get(account_actor.deposit.remote(33.))}")

# get actor id
print(ray.get_actor('Account'))

# kill the first instance
ray.kill(account_actor)

# and create a new one
account_actor = Account.options(name='Account').remote(balance=99.,minimal_balance=11.,
                                                       account_key='secretaccountkey', persistence=pool)

# it should have restored the state from before
print(f"INFO :: Current balance {ray.get(account_actor.balance.remote())}")

# verify that this is a new actor
print(ray.get_actor('Account'))

# kill the first instance
ray.kill(account_actor)