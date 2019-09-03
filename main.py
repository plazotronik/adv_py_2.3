#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import psycopg2 as psy
import psycopg2.errorcodes
from psycopg2.sql import SQL, Identifier
from datetime import datetime
from tabulate import tabulate


# создает соединение и БД
def connect_and_createdb(login, password, server='localhost', port='5432', new_db='netology_db'):
    global connect_, cursor_
    with psy.connect(f'postgres://{login}:{password}@{server}:{port}/{login}') as conn:
        conn.autocommit = True
        with conn.cursor() as cur:
            try:
                cur.execute(SQL('create database {};').format(Identifier(new_db)))
            except psy.Error as err:
                if err.pgcode == psy.errorcodes.DUPLICATE_DATABASE:
                    return f'Database {new_db} already exists.'
                else:
                    print(f'{err.pgerror}\nCode error: {err.pgcode}. '
                          f'See on http://initd.org/psycopg/docs/errors.html')
            finally:
                connect_ = psy.connect(f'postgres://{login}:{password}@{server}:{port}/{new_db}')
                cursor_ = connect_.cursor()


def create_tabs(): # создает таблицы
    connect_.autocommit = True
    try:
        cursor_.execute(SQL('''create table student (
                             id serial primary key not null,
                             name varchar(100) not null,
                             gpa numeric(10,2),
                             birth timestamptz);
                             create table course (
                             id serial primary key not null,
                             name varchar(100) not null);
                             create table student_course (
                             id serial primary key,
                             student_id integer references student(id) not null,
                             course_id integer references course(id) not null,
                             date date);'''))
    except psy.Error as err:
        if err.pgcode == psy.errorcodes.DUPLICATE_TABLE:
            return f'relations (tables) already exists'
        else:
            print(f'{err.pgerror}\nCode error: {err.pgcode}. '
                  f'See on http://initd.org/psycopg/docs/errors.html')


def add_courses(courses): # Добавляет курсы
    connect_.autocommit = True
    cursor_.executemany(SQL('insert into course (name) values (%s);'), courses)


def get_students(course_id): # возвращает студентов определенного курса
    connect_.autocommit = True
    cursor_.execute(SQL('''select student.id, student.name, student_course.date from student 
                        left join student_course on student.id = student_course.student_id 
                        where student_course.course_id = %s;'''), (course_id,))
    print('\n', tabulate(cursor_.fetchall(), headers=['ID', 'Name', 'Reception']))


def add_students(course_id, students): # создает студентов и записывает их на курс
    connect_.autocommit = True
    for stud in students:
        cursor_.execute(SQL('''insert into student_course (student_id, course_id, date) values (%s, %s, %s)'''),
                        (add_student(stud), course_id, datetime.date(datetime.now())))


def add_student(student): # просто создает студента
    connect_.autocommit = True
    if type(student) == dict:
        cursor_.execute(SQL('''insert into student (name, gpa, birth) 
                            values (%(name)s, %(gpa)s, %(birth)s) returning id;'''), student)
        return cursor_.fetchone()[0]
    elif type(student) in (tuple, list):
        cursor_.execute(SQL('''insert into student (name, gpa, birth) 
                            values (%s, %s, %s) returning id;'''), student)
        return cursor_.fetchone()[0]
    else:
        raise ValueError


def get_student(student_id): # получение информации о студенте
    connect_.autocommit = True
    cursor_.execute(SQL('''select * from student where id=%s;'''), (student_id,))
    print('\n', tabulate(cursor_.fetchall(), headers=['ID', 'Name', 'GPA', 'Birthday']))


if __name__ == '__main__':
    # input your credentials
    connect_and_createdb('your_login', 'your_password', 'your_server')
    create_tabs()

    curses_netology = [('Программирование на Python',), ('Программирование на JavaScript',),
               ('Программирование на Go',), ('Программирование на C++',)]
    add_courses(curses_netology)

    students_netology = [{'name': 'Иванов Иван Иванович', 'gpa': 4.72, 'birth': '1990-06-15 10:24:15+07'},
                         {'name': 'Петров Пётр Петрович', 'gpa': 4.83, 'birth': '1989-10-13 19:11:00+10'},
                         {'name': 'Семенов Семен Семенович', 'gpa': 4.65, 'birth': '1991-03-24 21:27:35+07'},
                         {'name': 'Фёдоров Фёдор Фёдорович', 'gpa': 3.98, 'birth': '1987-09-18 08:41:01+08'},]
    add_students(3, students_netology)
    get_students(3)

    student1 = {'name': 'Васичкин Василий Васильевич', 'gpa': 4.93, 'birth': '1990-11-23 14:00:03+05'}
    add_student(student1)
    student2 = ['Григорьев Григорий григорьевич', 4.41, '1988-04-28 09:31:20+06']
    add_student(student2)

    get_student(5)
    get_student(6)

    cursor_.close()
