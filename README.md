# Aloha


Aloha (/ɑːˈloʊhɑː/; Hawaiian: [əˈloːˌha]) is the Hawaiian word for love, affection, peace, compassion and mercy, that is commonly used as a simple greeting but has a deeper cultural and spiritual significance to native Hawaiians, in which the term is used to define a force that holds together existence. [Wikipedia]

Aloha app helps you diving into randomness of data provided by randomuser.me API. Read more to truly experience the power of randomness, the power of Aloha.

# Features!
    
  - Average age of selected gender;
 for your unique experience Aloha give you insight into months and days of length of random people lifes
  - Aloha will provide to you gender percentage share in total number of users
  - With Aloha you can also find out which cities do random people most often choose to live their random lives
  - Apart of that to satisfy your curiosity, with Aloha you will get grasp of how random people protect their privacy, that means you can find out most often choosen passwords
  - As Aloha cares about random people privacy, it gives guidance which passwords are safe with our state-of-the-art PasswordRanking valuation algorithm
- With batteries included Aloha provide you a 1000 records of data about random people
- You might also be interested in downloading some data straight from the randomuser.me API, but be careful as grabbing to much at once might suspend your IP for a while

### Tech
For all of you technology freaks, there are some open source projects that Aloha stands on. Apart of Python language it is:
- sqlalchemy
- tqdm

### Installation
Aloha requires above libraries to run- you'll find them in the requirements.txt file.
Once you install them you can start Aloha. To do so go to a folder where aloha.py file is and run it. For example, if you want to use provided records for your source of randomness, finding out average lif lenght of male randomers is simple as that:

```sh
$ python aloha.py -avg male
```
If you decide to download some data, let's say about 3000 randomers, do it like that:

```sh
$ python aloha.py -dl 3000 -cts 20
```
It will provide you 20 most common living choices of random people.

To find out more about at what aspects of random people lifes you can look at go for help as below:

```sh
$ python aloha.py -h
```

**Enjoy, Aloha!**