# Generated by Django 2.0.5 on 2018-05-24 09:50

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('masterdata', '0005_auto_20180523_1149'),
    ]

    operations = [
        migrations.AlterModelOptions(
            name='category',
            options={'ordering': ('name',), 'verbose_name_plural': 'categories'},
        ),
        migrations.AlterModelOptions(
            name='characteristic',
            options={'ordering': ('name',)},
        ),
        migrations.AlterModelOptions(
            name='characteristictype',
            options={'ordering': ('name',)},
        ),
        migrations.AlterModelOptions(
            name='group',
            options={'ordering': ('name',)},
        ),
        migrations.AlterModelOptions(
            name='ingredient',
            options={'ordering': ('name',)},
        ),
        migrations.AlterModelOptions(
            name='state',
            options={'ordering': ('name',)},
        ),
        migrations.AddField(
            model_name='ingredient',
            name='finalspec',
            field=models.ManyToManyField(blank=True, related_name='ingredient_finalspec', to='masterdata.Ingredient'),
        ),
        migrations.AddField(
            model_name='ingredient',
            name='notes',
            field=models.CharField(blank=True, max_length=100, null=True),
        ),
        migrations.AddField(
            model_name='ingredient',
            name='shelflife',
            field=models.IntegerField(blank=True, null=True),
        ),
    ]
