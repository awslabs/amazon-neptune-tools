import Image from 'next/image'

export default function Banner() {
    return (
        <Image
            src="/public/images/neptune.jpg" // Route of the image file
            height={144} // Desired size with correct aspect ratio
            width={144} // Desired size with correct aspect ratio
            alt="Neptune"
        />)
}
