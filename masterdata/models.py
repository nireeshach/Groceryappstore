from django.db import models
from django.utils import timezone
from django.db.models.signals import m2m_changed, post_save
from django.core.validators import MaxValueValidator, MinValueValidator

TRUE_FALSE_CHOICES = (
    (True, 'Yes'),
    (False, 'No')
)

# Create your models here.


class Characteristic(models.Model):
    id = models.AutoField(primary_key=True)
    name = models.CharField(max_length=100, unique=True, null=False)
    type = models.ForeignKey('CharacteristicType', on_delete=models.CASCADE,
                             null=True)
    alternates = models.TextField(blank=True, null=True)
    additional_info = models.TextField(blank=True, null=True)
    createdate = models.DateTimeField(auto_now_add=True)
    lastmodified = models.DateTimeField(auto_now=True)

    def __str__(self):
        return self.name

    class Meta:
        db_table = 'characteristic'
        ordering = ('name',)


class CharacteristicType(models.Model):
    id = models.AutoField(primary_key=True)
    name = models.CharField(max_length=100, unique=True, null=False)
    createdate = models.DateTimeField(auto_now_add=True)
    lastmodified = models.DateTimeField(auto_now=True)

    def __str__(self):
        return self.name

    class Meta:
        db_table = 'characteristic_type'
        ordering = ('name',)


class Ingredient(models.Model):
    id = models.AutoField(primary_key=True)
    name = models.CharField(max_length=100, unique=True, null=False)
    state = models.ForeignKey('State', on_delete=models.CASCADE, null=True)
    category = models.ForeignKey(
        'Category', on_delete=models.CASCADE, null=True)
    group = models.ForeignKey('Group', on_delete=models.CASCADE, null=True)
    alternates = models.ManyToManyField('self', symmetrical=False, blank=True)
    shelflife = models.IntegerField(blank=True, null=True,
                                    validators=[MinValueValidator(0), MaxValueValidator(100)])
    finalspec = models.ManyToManyField('self', symmetrical=False,
                                       blank=True, related_name='ingredient_finalspec')
    size_finalspec = models.ManyToManyField(
        'Size', blank=True, related_name='size_finalspec')
    notes = models.CharField(max_length=250, blank=True, null=True)
    additional_info = models.TextField(blank=True, null=True)
    validated = models.BooleanField(
        choices=TRUE_FALSE_CHOICES, default=False, blank=False)
    isalternate = models.BooleanField(
        choices=TRUE_FALSE_CHOICES, default=False, blank=False)
    isfinalspec = models.BooleanField(
        choices=TRUE_FALSE_CHOICES, default=False, blank=False)
    alternates_str = models.CharField(max_length=250, blank=True, null=True)
    finalspec_str = models.CharField(max_length=250, blank=True, null=True)
    size_str = models.CharField(max_length=250, blank=True, null=True)
    createdate = models.DateTimeField(auto_now_add=True)
    lastmodified = models.DateTimeField(auto_now=True)

    def get_alternates(self):
        return "; ".join([i['name'] for i in self.alternates.all().values('name')])
    get_alternates.short_description = 'alternates'

    def get_finalspec(self):
        return "; ".join([i['name'] for i in self.finalspec.all().values('name')])
    get_finalspec.short_description = 'finalspec'

    def get_size(self):
        return "; ".join([i['name'] for i in self.size_finalspec.all().values('name')])
    get_size.short_description = 'size'

    def __str__(self):
        return self.name

    class Meta:
        db_table = 'ingredient'
        ordering = ('name',)


class State(models.Model):
    id = models.AutoField(primary_key=True)
    name = models.CharField(max_length=100, unique=True, null=False)
    createdate = models.DateTimeField(auto_now_add=True)
    lastmodified = models.DateTimeField(auto_now=True)

    def __str__(self):
        return self.name

    class Meta:
        db_table = 'state'
        ordering = ('name',)


class Category(models.Model):
    id = models.AutoField(primary_key=True)
    name = models.CharField(max_length=100, unique=True, null=False)
    createdate = models.DateTimeField(auto_now_add=True)
    lastmodified = models.DateTimeField(auto_now=True)

    def __str__(self):
        return self.name

    class Meta:
        db_table = 'category'
        verbose_name_plural = 'categories'
        ordering = ('name',)


class Group(models.Model):
    id = models.AutoField(primary_key=True)
    name = models.CharField(max_length=100, unique=True, null=False)
    createdate = models.DateTimeField(auto_now_add=True)
    lastmodified = models.DateTimeField(auto_now=True)

    def __str__(self):
        return self.name

    class Meta:
        db_table = 'group'
        ordering = ('name',)


class Size(models.Model):
    id = models.AutoField(primary_key=True)
    name = models.CharField(max_length=100, unique=True, null=False)
    createdate = models.DateTimeField(auto_now_add=True)
    lastmodified = models.DateTimeField(auto_now=True)

    def __str__(self):
        return self.name

    class Meta:
        db_table = 'size'
        ordering = ('name',)


class IngredientConversion(models.Model):
    id = models.AutoField(primary_key=True)
    ingredient = models.CharField(max_length=100, null=False)
    preparation = models.CharField(max_length=100, null=False)
    size = models.CharField(max_length=100, null=False)
    category = models.CharField(max_length=100, null=False)
    form = models.CharField(max_length=100, null=False)
    cup_per_unit = models.FloatField(max_length=100, null=False)
    createdate = models.DateTimeField(auto_now_add=True)
    lastmodified = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = 'ingredent_conversion'
        ordering = ('ingredient',)
        unique_together = ('ingredient', 'preparation',
                           'size', 'category', 'form',)


class Pattern(models.Model):
    id = models.AutoField(primary_key=True)
    pattern = models.TextField(max_length=100, null=False)
    pattern_type = models.CharField(max_length=100, null=False, blank=True)
    target_pattern = models.TextField(max_length=100, null=False, blank=True)
    createdate = models.DateTimeField(auto_now_add=True)
    lastmodified = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = 'pattern'

# Django Signals


def ingredient_m2m(sender, action, instance, *args, **kwargs):
    if action == "pre_add" or action == "pre_remove":
        if sender in [Ingredient.alternates.through, Ingredient.finalspec.through]:
            added, deleted = [], []
            if action == "pre_remove":
                deleted = list(kwargs['pk_set'])
            else:
                # Skipping adding same Ingredient as its alternative
                if instance.id in kwargs['pk_set']:
                    kwargs['pk_set'].remove(instance.id)
                added_values = kwargs['pk_set']
                old_values = set(
                    list(instance.alternates.values_list('pk', flat=True)))
                added = list(added_values.difference(old_values))

            if len(deleted) > 0:
                deleted_objs = Ingredient.objects.filter(id__in=deleted)
                if Ingredient.alternates.through == sender:
                    # Setting isalternate to false in Alternative Ingredient
                    deleted_objs.update(isalternate=False)
                elif Ingredient.finalspec.through == sender:
                    # Setting isfinalspec to false in Finalspec Ingredient
                    deleted_objs.update(isfinalspec=False)

            if len(added) > 0:
                added_objs = Ingredient.objects.filter(id__in=added)
                if Ingredient.alternates.through == sender:
                    # Setting isalternate to true in Alternative Ingredient
                    added_objs.update(isalternate=True)
                elif Ingredient.finalspec.through == sender:
                    # Setting isfinalspec to false in Finalspec Ingredient
                    added_objs.update(isfinalspec=True)

    if action == "post_add" or action == "post_remove":
        # To avoid maximum recursion depth exceeded problem
        # Disconnecting the signal and reconnecting after save
        post_save.disconnect(ingredient_m2m, sender=sender)
        if Ingredient.alternates.through == sender:
            instance.alternates_str = instance.get_alternates()
        elif Ingredient.finalspec.through == sender:
            instance.finalspec_str = instance.get_finalspec()
        elif Ingredient.size_finalspec.through == sender:
            instance.size_str = instance.get_size()

        instance.save()
        post_save.connect(ingredient_m2m, sender=sender)


m2m_changed.connect(ingredient_m2m, sender=Ingredient.alternates.through)
m2m_changed.connect(ingredient_m2m, sender=Ingredient.finalspec.through)
m2m_changed.connect(ingredient_m2m, sender=Ingredient.size_finalspec.through)
