import random
import string


def generate_verification_code():
    return ''.join(random.sample([str(random.randint(0, 9)) for _ in range(3)] + [random.choice(string.ascii_letters) for _ in range(2)], 5))
