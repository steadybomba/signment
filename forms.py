   from flask_wtf import FlaskForm
   from wtforms import StringField
   from wtforms.validators import DataRequired, Length

   class TrackForm(FlaskForm):
       tracking_number = StringField('Tracking Number', validators=[DataRequired(), Length(min=1, max=50)])
