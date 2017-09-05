from django.db import models
from django.utils import timezone

# Create your models here.
TYPE_CHOICES = (
    ('url', 'url'),
    ('text', 'text')
)


class Post(models.Model):
    tag = models.CharField(max_length=63, default='', blank=True, null=True)
    spider = models.CharField(max_length=63)
    url = models.CharField(max_length=1023, unique=True)
    author = models.ForeignKey('Netizen', on_delete=models.SET_NULL, to_field='name', null=True)
    publish_date = models.DateTimeField(default=timezone.now)
    last_update = models.DateTimeField(default=timezone.now)
    update_count = models.IntegerField(default=0)
    allow_update = models.BooleanField(default=True)

    # title_raw = models.CharField(max_length=1023)
    # title_cleaned = models.CharField(max_length=1023)
    # comment_raw = models.TextField()
    # comment_cleaned = models.TextField()

    quality = models.FloatField(default=0.0)
    category = models.CharField(max_length=31, null=True, blank=True)

    def __str__(self):
        return '<{}>{}'.format(self.spider, self.title[:20])


class Netizen(models.Model):
    name = models.CharField(max_length=63, unique=True)
    category = models.CharField(max_length=31, null=True, blank=True)
    quality = models.FloatField(default=0.0)
    posts = models.IntegerField(default=0)
    comments = models.IntegerField(default=0)

    class Meta:
        verbose_name = "Netizen"
        verbose_name_plural = "Netizen"

    def __str__(self):
        return self.name


class Content(models.Model):
    ctype = models.CharField(max_length=31, choices=TYPE_CHOICES, default='text')
    category = models.CharField(max_length=31, null=True, blank=True)
    tokenizer = models.CharField(max_length=63)
    tokenized = models.CharField(max_length=1023)
    grammar = models.CharField(max_length=1023)
    retrieval_count = models.IntegerField(default=0)
    post = models.ForeignKey('Post', on_delete=models.CASCADE)
    quality = models.FloatField(default=0.0)

    class Meta:
        verbose_name = "Content"
        verbose_name_plural = "Content"
        unique_together = ('post', 'tokenizer')

    def __str__(self):
        return '<{}>{}'.format(self.tokenizer, self.tokenized)


class Title(models.Model):
    ctype = models.CharField(max_length=31, choices=TYPE_CHOICES, default='text')
    category = models.CharField(max_length=31, null=True, blank=True)
    tokenizer = models.CharField(max_length=63)
    tokenized = models.CharField(max_length=1023)
    grammar = models.CharField(max_length=1023)
    retrieval_count = models.IntegerField(default=0)
    post = models.ForeignKey('Post', on_delete=models.CASCADE)
    quality = models.FloatField(default=0.0)

    class Meta:
        verbose_name = "Title"
        verbose_name_plural = "Title"
        unique_together = ('post', 'tokenizer')

    def __str__(self):
        return '<{}>{}'.format(self.tokenizer, self.tokenized)


class Comment(models.Model):
    ctype = models.CharField(max_length=31, choices=TYPE_CHOICES, default='text')
    category = models.CharField(max_length=31, null=True, blank=True)
    tokenizer = models.CharField(max_length=63)
    tokenized = models.CharField(max_length=4095)
    grammar = models.CharField(max_length=4095)
    retrieval_count = models.IntegerField(default=0)
    audience = models.ForeignKey('Netizen', on_delete=models.SET_NULL, to_field='name', null=True)
    floor = models.IntegerField(default=0)
    post = models.ForeignKey('Post', on_delete=models.CASCADE)
    quality = models.FloatField(default=0.0)

    class Meta:
        verbose_name = "Comment"
        verbose_name_plural = "Comment"
        unique_together = ('post', 'floor', 'tokenizer')

    def __str__(self):
        return '<{}>{}'.format(self.tokenizer, self.tokenized)


class Vocabulary(models.Model):
    word = models.CharField(max_length=1023)
    tokenizer = models.CharField(max_length=31)
    pos = models.CharField(max_length=31, blank=True, null=True)
    post = models.ManyToManyField('Post', blank=True)
    comment = models.ManyToManyField('Comment', blank=True)
    content = models.ManyToManyField('Content', blank=True)
    title = models.ManyToManyField('Title', blank=True)
    # postfreq = models.IntegerField(default=0)
    titlefreq = models.IntegerField(default=0)
    contentfreq = models.IntegerField(default=0)
    commentfreq = models.IntegerField(default=0)
    stopword = models.BooleanField(default=False)
    quality = models.FloatField(default=0.0)

    class Meta:
        verbose_name = 'VOCABULARY'
        verbose_name_plural = verbose_name
        unique_together = ('word', 'pos', 'tokenizer')

    def __str__(self):
        return '{}'.format(self.word)


class IP(models.Model):
    address = models.CharField(max_length=31, unique=True)
    netizen = models.ManyToManyField('Netizen', blank=True)


class Association(models.Model):
    vocabt = models.ForeignKey('Vocabulary', on_delete=models.CASCADE, related_name='wordpost')
    vocabc = models.ForeignKey('Vocabulary', on_delete=models.CASCADE, related_name='wordcomment')
    pxy = models.IntegerField(default=0, null=True, blank=True)
    pmi = models.FloatField(default=0.0, null=True, blank=True)
    confidence = models.FloatField(default=0.0, null=True, blank=True)
    tokenizer = models.CharField(max_length=31)

    class Meta:
        unique_together = ('vocabt', 'vocabc')

