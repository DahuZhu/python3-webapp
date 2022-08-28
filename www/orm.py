import asyncio, logging, aiomysql
def log(sql,args=()):
    logging.info('SQL:%s' % sql)
 
# 定义创建连接池的协程函数
async def create_pool(loop,**kw):
    logging.info('creart database connection pool...')
    global __pool
    __pool = await aiomysql.create_pool(
        host=kw.get('host','localhost'),
        port=kw.get('port',3306),
        user=kw['user'],
        password=kw['password'],
        db=kw['db'],
        charset=kw.get('charset','utf8'),
        autocommit=kw.get('autocommit',True),
        maxsize=kw.get('maxsize',10),
        minsize=kw.get('minsize',1),
        loop=loop
    )
 
# 演示创建连接池
# 创建事件循环对象
loop = asyncio.get_event_loop()
# 定义连接池的参数
kw = {'user':'www-data','password':'www-data','db':'awesome'}
# 创建数据库连接池，执行完毕以后就创建了全局的连接池对象__pool
# 在执行搜索的select协程函数和执行修改的execute函数需要调用连接池对象创建数据库浮标对象
loop.run_until_complete(create_pool(loop=loop,**kw))
 
# 定义select协程函数
async def select(sql,args,size=None):
    log(sql,args)
    global __pool
    with (await __pool) as conn:
        # 使用数据库连接池对象__pool创建数据库浮标对象
        cur = await conn.cursor(aiomysql.DictCursor)
        # 查询语句通配符为'?'需要转换成MySQL语句通配符'%s'
        await cur.execute(sql.replace('?','%s'),args or ())
        # 如果传递了参数size则获取查询结果的前几个，size为正整数
        # 否则返回所有查询结果
        if size:
            rs = await cur.fetchmany(size)
        else:
            rs = await cur.fetchall()
        # 关闭数据库浮标
        await cur.close()
        logging.info('rows returned: %s' % len(rs))
        return rs
# Insert,Update,Delete
async def execute(sql, args):
    log(sql)
    with (await __pool) as conn:
        try:
            cur = await conn.cursor()
            await cur.execute(sql.replace('?', '%s'), args)
            affected = cur.rowcount
            await cur.close()
        except BaseException as e:
            raise
        return affected
 
# 定义创建匹配符的函数，传递一个整数，返回为由num个'?'组成的字符串
def create_args_string(num):
    L = []
    for n in range(num):
        L.append('?')
    return ', '.join(L)
 
class ModelMetaclass(type):
 
    def __new__(cls, name, bases, attrs):
        # 排除Model类本身:
        if name=='Model':
            return type.__new__(cls, name, bases, attrs)
        # 获取table名称:
        # 从字典attrs中获取属性__table__,如果在类中没有定义这个属性则返回None
        # 如果在属性中没有定义但是我们可以从参数name中获取到就是类名
        # 例如我们对类User进行重新创建,在类User中已经定义了属性 __table__ = 'users'
        # 所以我们优先得到的表名就是'users',假如没有定义则就是类名'User'
        # 为了便于观察我们打印对类修改前的attrs字典
        print("修改前attrs:%s" % attrs)
        tableName = attrs.get('__table__', None) or name
        # 输出日志
        logging.info('found model: %s (table: %s)' % (name, tableName))
        # 获取所有的Field和主键名:
        # 定义一个空字典用于存储需要定义的类中除了默认属性以为定义的属性
        # 例如本次我们针对类User则使用mappings字典存储'id','email','passwd','admin','name','image','create_at'的属性
        # 它们对应的属性值是一个实例化以后的实例，例如id对应的属性值是通过类StringField实例化后的实例
        mappings = dict()
        # fields表用于存储普通的字段名称，即除了主键以外的其他字段
        # 例如针对User类则在fields存储字段'email','passwd','admin','name','image','create_at'的名称
        fields = []
        primaryKey = None
        # 以key,value的方式遍历attrs字典
        for k, v in attrs.items():
            if isinstance(v, Field):
                logging.info('  found mapping: %s ==> %s' % (k, v))
                mappings[k] = v
                # 然后通过实例的属性primary_key去找主键，如果找到了主键则赋值给primaryKey
                # 如果不是主键的字段则追加至fields这个list
                # 一个表只能有一个主键，如果有多个主键则抛出RuntimeError错误，错误提示为重复的主键
                if v.primary_key:
                    # 找到主键:
                    if primaryKey:
                        raise RuntimeError('Duplicate primary key for field: %s' % k)
                    primaryKey = k
                else:
                    fields.append(k)
        # 如果表内没有定义主键则抛出错误主键没有发现
        if not primaryKey:
            raise RuntimeError('Primary key not found.')
        # 已经把对应的key value存储至字典以后，从原attrs中属性中删除对应的key value
        # 如果不删除则类属性和实例属性会冲突
        # 例如类User需要从原attrs中删除key值为 'id','email','passwd','admin','name','image','create_at'的元素
        for k in mappings.keys():
            attrs.pop(k)
        # 把存储除主键之外的字典list元素加一个``
        # 例如原fields为   ['email', 'passwd', 'admin', 'name', 'image', 'created_at']
        # map经过匿名函数把list中的所有元素处理加符号``,处理以后得到一个惰性序列然后通过list输出
        # escaped_fields为 ['`email`', '`passwd`', '`admin`', '`name`', '`image`', '`created_at`']
        escaped_fields = list(map(lambda f: '`%s`' % f, fields))
        # 保存属性和列的映射关系
        attrs['__mappings__'] = mappings
        attrs['__table__'] = tableName
        # 主键属性名
        attrs['__primary_key__'] = primaryKey
        # 除主键外的属性名
        attrs['__fields__'] = fields
        # 构造默认的SELECT, INSERT, UPDATE和DELETE语句:
        attrs['__select__'] = 'select `%s`, %s from `%s`' % (primaryKey, ', '.join(escaped_fields), tableName)
        attrs['__insert__'] = 'insert into `%s` (%s, `%s`) values (%s)' % (tableName, ', '.join(escaped_fields), primaryKey, create_args_string(len(escaped_fields) + 1))
        attrs['__update__'] = 'update `%s` set %s where `%s`=?' % (tableName, ', '.join(map(lambda f: '`%s`=?' % (mappings.get(f).name or f), fields)), primaryKey)
        attrs['__delete__'] = 'delete from `%s` where `%s`=?' % (tableName, primaryKey)
        print("修改后attrs:%s" % attrs)
        return type.__new__(cls, name, bases, attrs)
 
class Model(dict, metaclass=ModelMetaclass):
    @classmethod
    # 原始select语句 'select `id`, `email`, `passwd`, `admin`, `name`, `image`, `created_at` from `users`'
    # 原始是一个str,放在一个list中,即把这个str作为list的一个元素
    async def findAll(cls, where=None, args=None, **kw):
        ## find objects by where clause
        sql = [cls.__select__]
        if where:
            sql.append('where')
            sql.append(where)
        if args is None:
            args = []
        orderBy = kw.get('orderBy', None)
        if orderBy:
            sql.append('order by')
            sql.append(orderBy)
        limit = kw.get('limit', None)
        if limit is not None:
            sql.append('limit')
            if isinstance(limit, int):
                sql.append('?')
                args.append(limit)
            elif isinstance(limit, tuple) and len(limit) == 2:
                sql.append('?, ?')
                args.extend(limit)
            else:
                raise ValueError('Invalid limit value: %s' % str(limit))
        rs = await select(' '.join(sql), args)
        # 查询结果是一个list其中的元素是字典
        # 使用列表生成器把字典代入类中返回一个实例为原始的list
        return [cls(**r) for r in rs]
 
    # selectField传递一个字段名称例如id
    # 通过该字段去查询然后把查询到的第一个结果对应的值返回
    @classmethod
    async def findNumber(cls, selectField, where=None, args=None):
        ## find number by select and where
        sql = ['select %s _num_ from `%s`' % (selectField, cls.__table__)]
        if where:
            sql.append('where')
            sql.append(where)
        rs = await select(' '.join(sql), args, 1)
        if len(rs) == 0:
            return None
        # 取该结果对应的值返回
        return rs[0]['_num_']
 
    @classmethod
    async def find(cls, pk):
        ## find object by primary key
        rs = await select('%s where `%s`=?' % (cls.__select__, cls.__primary_key__), [pk], 1)
        if len(rs) == 0:
            return None
        # print(type(cls(**rs[0])),type(rs[0]))
        # 注意rs[0]是一个字典,使用cls(**rs[0])相当于把这个字典传递给类创建一个实例
        # 虽然打印rs[0]和cls(**rs[0])看起来是一样的但是类型不一样，不能返回字典因为返回字典就无法调用类的方法
        return cls(**rs[0])
 
    def __init__(self, **kw):
        super(Model, self).__init__(**kw)
 
    # 使字典可以以属性方式取值
    def __getattr__(self, key):
        try:
            return self[key]
        except KeyError:
            raise AttributeError(r"'Model' object has no attribute '%s'" % key)
 
    def __setattr__(self, key, value):
        self[key] = value
 
    # 定义方法通过key取值
    def getValue(self, key):
        return getattr(self, key, None)
 
    def getValueOrDefault(self, key):
        # 首先通过key从字典取值
        value = getattr(self, key, None)
        #  如果没有取得值,则从属性__mappings__中去获取
        if value is None:
            field = self.__mappings__[key]
            # 如果对应的实例的default值不为空，则判断如果是可执行对象则加括号执行，取得执行后的值
            # 否则取默认值
            if field.default is not None:
                value = field.default() if callable(field.default) else field.default
                logging.debug('using default value for %s: %s' % (key, str(value)))
                setattr(self, key, value)
        return value
 
    async def save(self):
        args = list(map(self.getValueOrDefault, self.__fields__))
        args.append(self.getValueOrDefault(self.__primary_key__))
        rows = await execute(self.__insert__, args)
        if rows != 1:
            logging.warning('failed to insert record: affected rows: %s' % rows)
 
    async def update(self):
        args = list(map(self.getValue, self.__fields__))
        args.append(self.getValue(self.__primary_key__))
        rows = await execute(self.__update__, args)
        if rows != 1:
            logging.warning('failed to update by primary key: affected rows: %s' % rows)
 
    async def remove(self):
        args = [self.getValue(self.__primary_key__)]
        rows = await execute(self.__delete__, args)
        if rows != 1:
            logging.warning('failed to remove by primary key: affected rows: %s' % rows)
     
# 定义Field类，作为数据库字段类型的父类
class Field(object):
    # 初始化方法定义了4个属性，分别为name字段名(id),column_type字段属性(bigint),primary_key是否为主键(True or False),default默认值
    def __init__(self, name, column_type, primary_key, default):
        self.name = name
        self.column_type = column_type
        self.primary_key = primary_key
        self.default = default
    # 返回实例对象的时候好看一点默认返回为 <__main__.StringField object at 0x0000025CC313EF08>
    # 定义了__str__返回为 <StringField:email>
    # 可以省略使用默认也可以
    def __str__(self):
        return '<%s, %s:%s>' % (self.__class__.__name__, self.column_type, self.name)
 
# 定义字符串字段类,继承父类Field的初始化方法，name字段名属性默认为None，到时使用类User创建出来的实例的key就是字段名
# 例如创建了一个User实例，该实例是一个字典,它包含的key有id email name等就是数据库表的字段名
class StringField(Field):
 
    def __init__(self, name=None, primary_key=False, default=None, ddl='varchar(100)'):
        super().__init__(name, ddl, primary_key, default)
 
# 定义布尔类型字段类，通过name属性为None
# 字段类型默认为boolean，默认为非主键，默认值为False
class BooleanField(Field):
 
    def __init__(self, name=None, default=False):
        super().__init__(name, 'boolean', False, default)
 
# 定义整数类型字段类
class IntegerField(Field):
 
    def __init__(self, name=None, primary_key=False, default=0):
        super().__init__(name, 'bigint', primary_key, default)
 
# 定义浮点类型字段类，字段类型real相当于float
class FloatField(Field):
 
    def __init__(self, name=None, primary_key=False, default=0.0):
        super().__init__(name, 'real', primary_key, default)
         
# 定义文本类型字段类
class TextField(Field):
 
    def __init__(self, name=None, default=None):
        super().__init__(name, 'text', False, default)