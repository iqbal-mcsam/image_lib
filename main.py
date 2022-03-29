import io
import json
from uuid import uuid4
import uuid
from boto3 import client
import ffmpeg
from ffprobe import FFProbe
from PIL import Image
from flask import Flask, request, jsonify
app = Flask(__name__)


class S3(object):
    """Example class demonstrating operations on S3"""

    bucket_name = "your bucket"
    aws_access_key_id = "your key"
    aws_secret_access_key = 'your access'

    def __init__(self, *args, **kwargs):
        region = kwargs.get('region_name', 'ap-south-1')
        self.bucket_name = kwargs.get('bucket_name', self.bucket_name)
        self.aws_access_key_id = kwargs.get(
            'aws_access_key_id', self.aws_access_key_id)
        self.aws_secret_access_key = kwargs.get(
            'aws_secret_access_key', self.aws_secret_access_key)
        self.conn = client('s3', region_name=region, aws_access_key_id=self.aws_access_key_id,
                           aws_secret_access_key=self.aws_secret_access_key)

    def upload_object(self, body, s3_key):
        """
        Upload object to s3 key
        """
        s3_client = self.conn
        return s3_client.put_object(Bucket=self.bucket_name, Body=body, Key=s3_key)

    def read_to_buffer(self, key):
        """
        Reading file return content type and file data 
        """
        s3_client = self.conn
        s3_response_object = s3_client.get_object(
            Bucket=self.bucket_name, Key=key)
        return [s3_response_object['ContentType'], s3_response_object['Body'].read()]

    def upload_file(self, file_name, key):
        self.conn.upload_file(file_name, self.bucket_name, key)

    def download_file(self, file_name, key):
        s3_client = self.conn
        return s3_client.download_file(self.bucket_name, key, file_name)


def create_watermark(event):
    try:
        file_name = event.get('file_name', None)
        # print(file_name)
        source_key = event.get('source_key', None)
        # print(source_key)
        watermark_key = event.get('watermark_key', None)
        # print(watermark_key)

        if file_name.endswith('.png') or file_name.endswith('jpg') or file_name.endswith('jpeg'):
            s3_instance = S3()
            source_content_type, source_file = s3_instance.read_to_buffer(
            source_key)
            _, watermark_file = s3_instance.read_to_buffer(watermark_key)

            main_image = Image.open(io.BytesIO(source_file))
            watermark_image = Image.open(io.BytesIO(watermark_file))
            width, height = main_image.size
            watermark = watermark_image.resize(
                (int(width / 4), int(height / 5)))
            watermark_width, watermark_height = watermark.size
            main_image.paste(watermark, (width - watermark_width,
                                         height - watermark_height), watermark)
            in_mem_file = io.BytesIO()
            main_image.save(in_mem_file, format=main_image.format)
            s3_instance.upload_object(
                body=in_mem_file.getvalue(), s3_key=source_key)
            return {
                'statusCode': 200,
                'body': {"status": "true", "message": "pasted logo successfully"}
            }
        if file_name.endswith('mp4') or file_name.endswith('avi'):
            s3_instance = S3()
            file_name = uuid.uuid4().hex + '.mp4'
            watermark = uuid.uuid4().hex + '.png'
            s3_instance.download_file(file_name=file_name, key=source_key)
            # s3_instance.download_file(file_name=watermark, key=watermark_key)
            in_file = ffmpeg.input(file_name)
            overlay_file = ffmpeg.input('ezgif.com-gif-maker.png')
            audio = in_file.audio
            out = uuid.uuid4().hex + 'out' + '.mp4'

            metadata = FFProbe(file_name)

            for stream in metadata.streams:
                if stream.is_video():
                    width, height = stream.frame_size()
            
            image_metadata = FFProbe('ezgif.com-gif-maker.png')
            for stream in image_metadata.streams:
                if stream.is_video():
                    image_width, image_height = stream.frame_size()

            # y=27% , x=54%
            # main_w-overlay_w-(main_w*0.01):y=main_h-overlay_h-(main_h*0.01)
            x = width - image_width - (width*0.01)
            y = height - image_height - (height*0.01)
            (
            ffmpeg
            .filter([in_file, overlay_file], 'overlay')
            .output(audio, out)
            .run()
            )
            # s3_instance.upload_file(
            #     file_name=out, key=source_key)
            
            return {
                'statusCode': 200,
                'body': {"status": "true", "message": "logo pasted successfully"}
            }

    except Exception as e:
        return {
            'statusCode': 500,
            'body': {"status": "false", "message": str(e)}
        }


@app.route("/", methods=['POST'])
def watermark():
    res = create_watermark(request.json)
    print(res)
    return jsonify(res)


if __name__ == "__main__":
    app.run()



