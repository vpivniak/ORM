import psycopg2
from my_entity import *


class Section(Entity):
    _columns = ['title']
    _parents = []
    _children = {'categories': 'Category'}
    _siblings = {}


class Category(Entity):
    _columns = ['title']
    _parents = ['section']
    _children = {'posts': 'Post'}
    _siblings = {}


class Post(Entity):
    _columns = ['content', 'title']
    _parents = ['category']
    _children = {'comments': 'Comment'}
    _siblings = {'tags': 'Tag'}


class Comment(Entity):
    _columns = ['text']
    _parents = ['post', 'user']
    _children = {}
    _siblings = {}


class Tag(Entity):
    _columns = ['name']
    _parents = []
    _children = {}
    _siblings = {'posts': 'Post'}


class User(Entity):
    _columns = ['name', 'email', 'age']
    _parents = []
    _children = {'comments': 'Comment'}
    _siblings = {}


if __name__ == "__main__":
    Entity.db = psycopg2.connect(database="orm_base", user="user", password="pass", host="127.0.0.1", port="5433")

    # section = Section(1)
    # # section.title = 'test'
    # # section.save()

    category = Category(18)
    # # # category.title = 'categpry test'
    # # # category.section = section
    # # # category.save()
    #
    post = Post()
    post.title = 'any_post_title_1'
    post.category = category
    post.save()

    post2 = Post()
    post2.title = 'any_post_title_2'
    post2.category = category
    post2.save()

    tag = Tag()
    tag.name = 'many posts'
    tag.posts = [post, post2]
    tag.save()

    tag2 = Tag(3)
    tag3 = Tag(4)
    tag4 = Tag(10)

    post3 = Post(46)
    post3.tags = [tag2, tag3, tag4]
    post3.save()
    #
    # category2 = Category(11)
    # for post_c in category2.posts:
    #     print(f'Post title: {post_c.title}')
    #
    # post = Post()
    # post.title = 'titltlt'
    # post.category = category
    # tags = Tag().all()
    # print(len(tags))
    # post.tags = tags
    # post.save()
    #
    # for tag in post.tags:
    #     print(tag.name)

    Entity.db.close()
